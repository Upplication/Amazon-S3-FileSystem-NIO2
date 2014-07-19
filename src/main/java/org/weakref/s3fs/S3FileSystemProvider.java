package org.weakref.s3fs;

import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Spec:
 * 
 * URI: s3://[endpoint]/{bucket}/{key} If endpoint is missing, it's assumed to
 * be the default S3 endpoint (s3.amazonaws.com)
 * 
 * FileSystem roots: /{bucket}/
 * 
 * Treatment of S3 objects: - If a key ends in "/" it's considered a directory
 * *and* a regular file. Otherwise, it's just a regular file. - It is legal for
 * a key "xyz" and "xyz/" to exist at the same time. The latter is treated as a
 * directory. - If a file "a/b/c" exists but there's no "a" or "a/b/", these are
 * considered "implicit" directories. They can be listed, traversed and deleted.
 * 
 * Deviations from FileSystem provider API: - Deleting a file or directory
 * always succeeds, regardless of whether the file/directory existed before the
 * operation was issued i.e. Files.delete() and Files.deleteIfExists() are
 * equivalent.
 * 
 * 
 * Future versions of this provider might allow for a strict mode that mimics
 * the semantics of the FileSystem provider API on a best effort basis, at an
 * increased processing cost.
 * 
 * 
 */
public class S3FileSystemProvider extends FileSystemProvider {
	
	public static final String ACCESS_KEY = "access_key";
	public static final String SECRET_KEY = "secret_key";

	final AtomicReference<S3FileSystem> fileSystem = new AtomicReference<>();

	@Override
	public String getScheme() {
		return "s3";
	}

	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env)
			throws IOException {
		Preconditions.checkNotNull(uri, "uri is null");
		Preconditions.checkArgument(uri.getScheme().equals("s3"),
				"uri scheme must be 's3': '%s'", uri);
		// first try to load amazon props
		Properties props = loadAmazonProperties();
		Object accessKey = props.getProperty(ACCESS_KEY);
		Object secretKey = props.getProperty(SECRET_KEY);
		// but can overload by envs vars
		if (env.get(ACCESS_KEY) != null){
			accessKey = env.get(ACCESS_KEY);
		}
		if (env.get(SECRET_KEY) != null){
			secretKey = env.get(SECRET_KEY);
		}
		
		Preconditions.checkArgument((accessKey == null && secretKey == null)
				|| (accessKey != null && secretKey != null),
				"%s and %s should both be provided or should both be omitted",
				ACCESS_KEY, SECRET_KEY);

		S3FileSystem result = createFileSystem(uri, accessKey, secretKey);
		// if this instance already has a S3FileSystem, throw exception
		// otherwise set
		if (!fileSystem.compareAndSet(null, result)) {
			throw new FileSystemAlreadyExistsException(
					"S3 filesystem already exists. Use getFileSystem() instead");
		}

		return result;
	}

	@Override
	public FileSystem getFileSystem(URI uri) {
		FileSystem fileSystem = this.fileSystem.get();

		if (fileSystem == null) {
			throw new FileSystemNotFoundException(
					String.format("S3 filesystem not yet created. Use newFileSystem() instead"));
		}

		return fileSystem;
	}

	/**
	 * Deviation from spec: throws FileSystemNotFoundException if FileSystem
	 * hasn't yet been initialized. Call newFileSystem() first.
	 * Need credentials. Maybe set credentials after? how?
	 */
	@Override
	public Path getPath(URI uri) {
		Preconditions.checkArgument(uri.getScheme().equals(getScheme()),
				"URI scheme must be %s", getScheme());

		if (uri.getHost() != null && !uri.getHost().isEmpty() &&
				!uri.getHost().equals(fileSystem.get().getEndpoint())) {
			throw new IllegalArgumentException(String.format(
					"only empty URI host or URI host that matching the current fileSystem: %s",
					fileSystem.get().getEndpoint())); // TODO
		}
		/**
		 * tener una lista: un s3fileSystem por region y posiblemente
		 * poder incluir en la url el acceso.
		 */
		return getFileSystem(uri).getPath(uri.getPath());
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir,
			DirectoryStream.Filter<? super Path> filter) throws IOException {

		Preconditions.checkArgument(dir instanceof S3Path,
				"path must be an instance of %s", S3Path.class.getName());
		final S3Path s3Path = (S3Path) dir;

		return new DirectoryStream<Path>() {
			@Override
			public void close() throws IOException {
				// nothing to do here
			}

			@Override
			public Iterator<Path> iterator() {
				return new Iterator<Path>() {

					private S3Path dir = s3Path;
					private Iterator<S3Path> it;

					@Override
					public void remove() {
						// not supported
					}

					@Override
					public Path next() {
						return getIterator().next();
					}

					@Override
					public boolean hasNext() {
						return getIterator().hasNext();
					}

					private Iterator<S3Path> getIterator() {
						if (it == null) {
							List<S3Path> listPath = new ArrayList<>();
							// TODO: need revision for better performance!
							ListObjectsRequest request = new ListObjectsRequest();
							request.setBucketName(s3Path.getBucket());
							request.setPrefix(s3Path.getKey());
							request.setMarker(s3Path.getKey());
							// carga TODOS los elementos a todos los niveles :(
							for (final S3ObjectSummary objectSummary : dir.getFileSystem().getClient().listObjects(request).getObjectSummaries()) {
								final String key = objectSummary.getKey();
								// filtramos para quedarnos con los de primer
								// nivel
								String folder = getInmediateDescendent(s3Path.getKey(), key);
								if (folder != null){
									S3Path descendentPart = new S3Path(dir.getFileSystem(), objectSummary.getBucketName(), folder.split("/"));
									
									if (!listPath.contains(descendentPart)){
										listPath.add(descendentPart);
									}
									
								}
								
							}
							it = listPath.iterator();
						}

						return it;
					}
					
					public String getInmediateDescendent(String keyParent, String keyChild){
						
						keyParent = deleteExtraPath(keyParent);
						keyChild = deleteExtraPath(keyChild);
						
						if (!keyChild.startsWith(keyParent)) {
							// maybe we just should return false
							throw new IllegalArgumentException(
									"Invalid child '" + keyChild
											+ "' for parent '" + keyParent + "'");
						}
						final int parentLen = keyParent.length();
						final String childWithoutParent = deleteExtraPath(keyChild
								.substring(parentLen));
						
						String[] parts = childWithoutParent.split("/");
						
						if (parts.length > 0 && !parts[0].isEmpty()){
							return keyParent + "/" + parts[0];
						}
						else{
							return null;
						}
							
					}

					private String deleteExtraPath(String keyChild) {
						if (keyChild.startsWith("/")){
							keyChild = keyChild.substring(1);
						}
						if (keyChild.endsWith("/")){
							keyChild = keyChild.substring(0, keyChild.length() - 1);
						}
						return keyChild;
					}
				};
			}

		};
	}

	@Override
	public InputStream newInputStream(Path path, OpenOption... options)
			throws IOException {
		Preconditions.checkArgument(options.length == 0,
				"OpenOptions not yet supported: %s",
				ImmutableList.copyOf(options)); // TODO

		Preconditions.checkArgument(path instanceof S3Path,
				"path must be an instance of %s", S3Path.class.getName());
		S3Path s3Path = (S3Path) path;

		Preconditions.checkArgument(!s3Path.getKey().equals(""),
				"cannot create InputStream for root directory: %s", s3Path);
	
		InputStream res = s3Path.getFileSystem().getClient()
				.getObject(s3Path.getBucket(), s3Path.getKey())
				.getObjectContent();
	
		if (res == null){
			throw new IOException("path is a directory");
		}
		else{
			return res;
		}
	}

	@Override
	public OutputStream newOutputStream(Path path, OpenOption... options)
			throws IOException {

		Preconditions.checkArgument(path instanceof S3Path,
				"path must be an instance of %s", S3Path.class.getName());
		final S3Path s3Path = (S3Path) path;
		
		final Path tempFile = Files.createTempFile("file", s3Path.getFileName().toString());

		return new FileOutputStream(tempFile.toFile()) {
			@Override
			public void close() throws IOException {
				super.close();
				
				s3Path.getFileSystem()
						.getClient()
						.putObject(s3Path.getBucket(), s3Path.getKey(),
								tempFile.toFile());

				Files.deleteIfExists(tempFile);
			}
		};
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path,
			Set<? extends OpenOption> options, FileAttribute<?>... attrs)
			throws IOException {
		Preconditions.checkArgument(path instanceof S3Path,
				"path must be an instance of %s", S3Path.class.getName());
		final S3Path s3Path = (S3Path) path;
		// creamos un fichero vacio:
		final Path tempDir = Files.createTempDirectory("temp-s3");
		// ahora podemos leer simulando las escrituras
		final Path file = tempDir.resolve(path.getFileName().toString());
		// FIXME: delete, windows bug?
		//Files.createFile(file);
		final SeekableByteChannel seekable = Files
				.newByteChannel(file, options);

		return new SeekableByteChannel() {
			@Override
			public boolean isOpen() {
				return seekable.isOpen();
			}

			@Override
			public void close() throws IOException {
				seekable.close();
				// guardmaos en el close
				// FIXME: comprobar que no existe una carpeta con el mismo nombre: si existe: lanzar exception
				s3Path.getFileSystem()
						.getClient()
						.putObject(s3Path.getBucket(), s3Path.getKey(),
								file.toFile());
				// eliminamos el fichero temporal que utilizamos de puente
				Files.walkFileTree(tempDir, new SimpleFileVisitor<Path>() {

					@Override
					public FileVisitResult visitFile(Path file,
							BasicFileAttributes attrs) throws IOException {
						Files.delete(file);
						return FileVisitResult.CONTINUE;
					}

					@Override
					public FileVisitResult postVisitDirectory(Path dir,
							IOException exc) throws IOException {
						if (exc == null) {
							Files.delete(dir);
							return FileVisitResult.CONTINUE;
						}
						throw exc;
					}
				});
			}

			@Override
			public int write(ByteBuffer src) throws IOException {
				return seekable.write(src);
			}

			@Override
			public SeekableByteChannel truncate(long size) throws IOException {
				return seekable.truncate(size);
			}

			@Override
			public long size() throws IOException {
				return seekable.size();
			}

			@Override
			public int read(ByteBuffer dst) throws IOException {
				return seekable.read(dst);
			}

			@Override
			public SeekableByteChannel position(long newPosition)
					throws IOException {
				return seekable.position(newPosition);
			}

			@Override
			public long position() throws IOException {
				return seekable.position();
			}
		};
	}

	/**
	 * Deviations from spec: Does not perform atomic check-and-create. Since a
	 * directory is just an S3 object, all directories in the hierarchy are
	 * created or it already existed.
	 */
	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs)
			throws IOException {
		
		// FIXME: comprobar que si ya existe un fichero con la misma key: no permitir.
		
		S3Path s3Path = (S3Path) dir;

		Preconditions.checkArgument(attrs.length == 0,
				"attrs not yet supported: %s", ImmutableList.copyOf(attrs)); // TODO

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);

		String keyName = s3Path.getKey()
				+ (s3Path.getKey().endsWith("/") ? "" : "/");

		s3Path.getFileSystem()
				.getClient()
				.putObject(s3Path.getBucket(), keyName,
						new ByteArrayInputStream(new byte[0]), metadata);
	}

	/**
	 * Deviations from spec: - does not check whether path exists before
	 * deleting it. I.e., the operation is considered successful whether the
	 * entry was deleted or it didn't exist in the first place - doesn't throw
	 * DirectoryNotEmptyException if the path is a directory and it contains
	 * entries
	 */
	@Override
	public void delete(Path path) throws IOException {
		Preconditions.checkArgument(path instanceof S3Path,
				"path must be an instance of %s", S3Path.class.getName());

		S3Path s3Path = (S3Path) path;
		// borramos los dos:
		s3Path.getFileSystem().getClient()
			.deleteObject(s3Path.getBucket(), s3Path.getKey());
		s3Path.getFileSystem().getClient()
			.deleteObject(s3Path.getBucket(), s3Path.getKey() + "/");
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options)
			throws IOException {
		Preconditions.checkArgument(source instanceof S3Path,
				"source must be an instance of %s", S3Path.class.getName());
		Preconditions.checkArgument(target instanceof S3Path,
				"target must be an instance of %s", S3Path.class.getName());

		if (isSameFile(source, target)) {
			return;
		}

		S3Path s3Source = (S3Path) source;
		S3Path s3Target = (S3Path) target;
		/*
		 * Preconditions.checkArgument(!s3Source.isDirectory(),
		 * "copying directories is not yet supported: %s", source); // TODO
		 * Preconditions.checkArgument(!s3Target.isDirectory(),
		 * "copying directories is not yet supported: %s", target); // TODO
		 */
		ImmutableSet<CopyOption> actualOptions = ImmutableSet.copyOf(options);
		verifySupportedOptions(EnumSet.of(StandardCopyOption.REPLACE_EXISTING),
				actualOptions);

		if (!actualOptions.contains(StandardCopyOption.REPLACE_EXISTING)) {
			if (exists(s3Target)) {
				throw new FileAlreadyExistsException(format(
						"target already exists: %s", target));
			}
		}

		s3Source.getFileSystem()
				.getClient()
				.copyObject(s3Source.getBucket(), s3Source.getKey(),
						s3Target.getBucket(), s3Target.getKey());
	}

	@Override
	public void move(Path source, Path target, CopyOption... options)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSameFile(Path path1, Path path2) throws IOException {
		return path1.isAbsolute() && path2.isAbsolute() && path1.equals(path2);
	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		return false;
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		S3Path s3Path = (S3Path) path;
		Preconditions.checkArgument(s3Path.isAbsolute(),
				"path must be absolute: %s", s3Path);

		AmazonS3Client client = s3Path.getFileSystem().getClient();

		// get ACL and check if the file exists as a side-effect
		AccessControlList acl = getAccessControl(s3Path);

		for (AccessMode accessMode : modes) {
			switch (accessMode) {
			case EXECUTE:
				throw new AccessDeniedException(s3Path.toString(), null,
						"file is not executable");
			case READ:
				if (!hasPermissions(acl, client.getS3AccountOwner(),
						EnumSet.of(Permission.FullControl, Permission.Read))) {
					throw new AccessDeniedException(s3Path.toString(), null,
							"file is not readable");
				}
				break;
			case WRITE:
				if (!hasPermissions(client.getBucketAcl(s3Path.getBucket()),
						client.getS3AccountOwner(),
						EnumSet.of(Permission.FullControl, Permission.Write))) {
					throw new AccessDeniedException(s3Path.toString(), null,
							format("bucket '%s' is not writable",
									s3Path.getBucket()));
				}
				break;
			default:
				throw new UnsupportedOperationException(format(
						"access mode '%s' not supported", accessMode));
			}
		}
	}

	private boolean hasPermissions(AccessControlList acl, Owner owner,
			EnumSet<Permission> permissions) {
		boolean result = false;
		for (Grant grant : acl.getGrants()) {
			if (grant.getGrantee().getIdentifier().equals(owner.getId())
					&& permissions.contains(grant.getPermission())) {
				result = true;
				break;
			}
		}
		return result;
	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path,
			Class<V> type, LinkOption... options) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path,
			Class<A> type, LinkOption... options) throws IOException {
		Preconditions.checkArgument(path instanceof S3Path,
				"path must be an instance of %s", S3Path.class.getName());
		S3Path s3Path = (S3Path) path;

		if (type == BasicFileAttributes.class) {

			S3ObjectSummary objectSummary = getFirstObjectSummary(s3Path);

			// transformamos los datos en BasicFileAttributes.
			
			FileTime lastModifiedTime = FileTime.from(objectSummary.getLastModified().getTime(),
					TimeUnit.MILLISECONDS);
			long size =  objectSummary.getSize();
			boolean directory = false;
			boolean regularFile = true;
			String key = objectSummary.getKey();
			// puede que exista el key del folder y debe tener barra al final:
			if (objectSummary.getKey().equals(s3Path.getKey()) && objectSummary.getKey().endsWith("/")) {
				directory = true;
			}
			// es un subfichero: es un directorio
			else if (!objectSummary.getKey().equals(s3Path.getKey()) && objectSummary.getKey().startsWith(s3Path.getKey())){
				directory = true;
				// nos "inventamos" el metadata
				size = 0;
			}
			// es un fichero:
			else if (objectSummary.getKey().equals(s3Path.getKey())){
				directory = false;
			}
			else {
				throw new NoSuchFileException(path.toString());
			}
			
			return type.cast(new S3FileAttributes(key, lastModifiedTime, size, directory, regularFile));
		}

		return null;
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes,
			LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value,
			LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	// ~~
	/**
	 * Create the fileSystem
	 * @param uri URI
	 * @param accessKey Object maybe null for anonymous authentication
	 * @param secretKey Object maybe null for anonymous authentication
	 * @return
	 */
	protected S3FileSystem createFileSystem(URI uri, Object accessKey,
			Object secretKey) {
		AmazonS3Client client;

		if (accessKey == null && secretKey == null) {
			client = new AmazonS3Client();
		} else {
			client = new AmazonS3Client(new BasicAWSCredentials(
					accessKey.toString(), secretKey.toString()));
		}

		if (uri.getHost() != null) {
			client.setEndpoint(uri.getHost());
		}

		S3FileSystem result = new S3FileSystem(this, client, uri.getHost());
		return result;
	}
	
	/**
	 * find /amazon.properties in the classpath
	 * @return
	 */
	protected Properties loadAmazonProperties() {
		Properties props = new Properties();
		// http://www.javaworld.com/javaworld/javaqa/2003-06/01-qa-0606-load.html
		// http://www.javaworld.com/javaqa/2003-08/01-qa-0808-property.html
		try(InputStream in = Thread.currentThread ().getContextClassLoader ().getResourceAsStream("amazon.properties")){
			if (in != null){
				props.load(in);
			}
			
		} catch (IOException e) {}
		
		return props;
	}
	
	// ~~~

	private <T> void verifySupportedOptions(Set<? extends T> allowedOptions,
			Set<? extends T> actualOptions) {
		Sets.SetView<? extends T> unsupported = difference(actualOptions,
				allowedOptions);
		Preconditions.checkArgument(unsupported.isEmpty(),
				"the following options are not supported: %s", unsupported);
	}
	/**
	 * Comprueba si existe
	 * @param path
	 * @return
	 */
	private boolean exists(S3Path path) {
		try{
			getFirstObjectSummary(path);
			return true;
		}
		catch(NoSuchFileException e){
			return false;
		}
	}
	/**
	 * Obtiene el {@link S3ObjectSummary} que representa este Path
	 * o su primer hijo (si no existe el object Path)
	 * @param s3Path {@link S3Path}
	 * @return {@link S3ObjectSummary}
	 * @throws NoSuchFileException si no se encuentra el path (tanto con barra como sin barra) y tampoco ningun hijo
	 */
	private S3ObjectSummary getFirstObjectSummary(S3Path s3Path) throws NoSuchFileException{
		
		S3ObjectSummary res = null;
		try {
			
			AmazonS3Client client = s3Path.getFileSystem().getClient();
			
			ListObjectsRequest request = new ListObjectsRequest();
			request.setBucketName(s3Path.getBucket());
			request.setPrefix(s3Path.getKey());
			request.setMaxKeys(1);
			List<S3ObjectSummary> query = client.listObjects(request).getObjectSummaries();
			if (!query.isEmpty()){
				res = query.get(0);
			}
			else{
				throw new NoSuchFileException(s3Path.toString());
			}
			
		} catch (AmazonS3Exception e) {
			if (e.getStatusCode() == 404) {
				throw new NoSuchFileException(s3Path.toString());
			}
			Throwables.propagate(e);
		}
		
		return res;
	}
	/**
	 * Obtiene el access Control list, si no existe el object porque el path
	 * representa un directorio no creado en S3. devuelve el ACL del primer hijo o
	 * lanza NoSuchFileException
	 * @param path {@link S3Path}
	 * @return AccessControlList
	 * @throws NoSuchFileException si no encuentra el path (tanto con barra como sin barra) y tampoco ningun hijo
	 */
	private AccessControlList getAccessControl(S3Path path) throws NoSuchFileException{
		
		AccessControlList res = null;
		S3ObjectSummary obj = getFirstObjectSummary(path);
		
		try {
			// chek first for file:
			res = path.getFileSystem().getClient().getObjectAcl(obj.getBucketName(), obj.getKey());
		} catch (AmazonS3Exception e) {
			Throwables.propagate(e);
		}
		
		return res;
	}
}
