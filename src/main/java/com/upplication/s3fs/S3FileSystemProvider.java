package com.upplication.s3fs;

import static com.google.common.collect.Sets.difference;
import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.CONNECTION_TIMEOUT;
import static com.upplication.s3fs.AmazonS3Factory.MAX_CONNECTIONS;
import static com.upplication.s3fs.AmazonS3Factory.MAX_ERROR_RETRY;
import static com.upplication.s3fs.AmazonS3Factory.PROTOCOL;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_DOMAIN;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_HOST;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_PASSWORD;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_PORT;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_USERNAME;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_WORKSTATION;
import static com.upplication.s3fs.AmazonS3Factory.REQUEST_METRIC_COLLECTOR_CLASS;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_RECEIVE_BUFFER_SIZE_HINT;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_SEND_BUFFER_SIZE_HINT;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_TIMEOUT;
import static com.upplication.s3fs.AmazonS3Factory.USER_AGENT;
import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.upplication.s3fs.util.S3Utils;

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

	public static final String CHARSET_KEY = "s3fs_charset";
	public static final String AMAZON_S3_FACTORY_CLASS = "s3fs_amazon_s3_factory";

    private static final ConcurrentMap<String, S3FileSystem> fileSystems = new ConcurrentHashMap<>();

    private S3Utils s3Utils = new S3Utils();

	@Override
	public String getScheme() {
		return "s3";
	}

	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env) {
		validateUri(uri);
		// get properties for the env or properties or system
		Properties props = getProperties(uri, env);
		validateProperties(props);
        // try to get the filesystem by the key
		String key = getFileSystemKey(uri, props);
		if (fileSystems.containsKey(key)) {
            throw new FileSystemAlreadyExistsException("File system " + uri.getScheme() + ':' + key + " already exists");
        }
        // create the filesystem with the final properties, store and return
		S3FileSystem fileSystem = createFileSystem(uri, props);
		fileSystems.put(fileSystem.getKey(), fileSystem);
		return fileSystem;
	}

	private void validateProperties(Properties props) {
		Preconditions.checkArgument(
				(props.getProperty(ACCESS_KEY) == null && props.getProperty(SECRET_KEY) == null)
						|| (props.getProperty(ACCESS_KEY) != null && props.getProperty(SECRET_KEY) != null), "%s and %s should both be provided or should both be omitted",
				ACCESS_KEY, SECRET_KEY);
	}

	private Properties getProperties(URI uri, Map<String, ?> env) {
		Properties props = loadAmazonProperties();
		// but can be overloaded by envs vars
		overloadProperties(props, env);
        // and access key and secret key can be override
		String userInfo = uri.getUserInfo();
		if (userInfo != null) {
			String[] keys = userInfo.split(":");
			props.setProperty(ACCESS_KEY, keys[0]);
            if (keys.length > 1) {
                props.setProperty(SECRET_KEY, keys[1]);
            }
		}
		return props;
	}

	private String getFileSystemKey(URI uri) {
		return getFileSystemKey(uri, getProperties(uri, null));
	}

    /**
     * get the file system key represented by: the access key @ endpoint.
     * Example: access-key@s3.amazon.com
     * @param uri URI with the endpoint
     * @param props with the access key property
     * @return String
     */
	protected String getFileSystemKey(URI uri, Properties props) {
        String uriString = uri.toString().replace("s3://", "");
        String authority = null;
        int authoritySeparator = uriString.indexOf("@");

        if (authoritySeparator > 0){
            authority = uriString.substring(0, authoritySeparator);
        }

        if (authority != null) {
            String host = uriString.substring(uriString.indexOf("@")+1, uriString.length());
            int lastPath = host.indexOf("/");
            if (lastPath > 0){
                host = host.substring(0, lastPath);
            }
            return authority + "@" + host;
        }
        else {
            String accessKey = (String) props.get(ACCESS_KEY);

            return (accessKey != null ? accessKey+"@" : "" ) + uri.getHost();
        }
	}

	protected void validateUri(URI uri) {
		Preconditions.checkNotNull(uri, "uri is null");
		Preconditions.checkArgument(uri.getScheme().equals(getScheme()), "uri scheme must be 's3': '%s'", uri);
	}

	protected void overloadProperties(Properties props, Map<String, ?> env) {
		if (env == null)
			env = new HashMap<>();
		for (String key : new String[] { ACCESS_KEY, SECRET_KEY, REQUEST_METRIC_COLLECTOR_CLASS, CONNECTION_TIMEOUT, MAX_CONNECTIONS, MAX_ERROR_RETRY, PROTOCOL, PROXY_DOMAIN,
				PROXY_HOST, PROXY_PASSWORD, PROXY_PORT, PROXY_USERNAME, PROXY_WORKSTATION, SOCKET_SEND_BUFFER_SIZE_HINT, SOCKET_RECEIVE_BUFFER_SIZE_HINT, SOCKET_TIMEOUT,
				USER_AGENT, AMAZON_S3_FACTORY_CLASS }){
            overloadProperty(props, env, key);
        }
	}

    /**
     * try to override the properties props with:
     * <ol>
     *  <li>the map or if not setted:</li>
     *  <li>the system property or if not setted:</li>
     *  <li>the system vars</li>
     * </ol>
     * @param props Properties to override
     * @param env Map the first option
     * @param key String the key
     */
	private void overloadProperty(Properties props, Map<String, ?> env, String key) {
        boolean overloaded = overloadPropertiesWithEnv(props, env, key);

        if (!overloaded){
            overloaded = overloadPropertiesWithSystemProps(props, key);
        }

        if (!overloaded){
            overloadPropertiesWithSystemEnv(props, key);
        }
	}

    /**
     * @return true if the key are overloaded by the map parameter
     */
    protected boolean overloadPropertiesWithEnv(Properties props, Map<String, ?> env, String key){
        if (env.get(key) != null && env.get(key) instanceof String) {
            props.setProperty(key, (String) env.get(key));
            return true;
        }
        return false;
    }
    /**
     * @return true if the key are overloaded by a system property
     */
    protected boolean overloadPropertiesWithSystemProps(Properties props, String key){
        if (System.getProperty(key) != null) {
            props.setProperty(key, System.getProperty(key));
            return true;
        }
        return false;
    }
    /**
     * @return true if the key are overloaded by a system property
     */
    protected boolean overloadPropertiesWithSystemEnv(Properties props, String key){
        if (systemGetEnv(key) != null) {
            props.setProperty(key, systemGetEnv(key));
            return true;
        }
        return false;
    }

    protected String systemGetEnv(String key){
        return System.getenv(key);
    }

	public FileSystem getFileSystem(URI uri, Map<String, ?> env) {
		validateUri(uri);
		String key = this.getFileSystemKey(uri);
		if (fileSystems.containsKey(key))
			return fileSystems.get(key);
		return newFileSystem(uri, env);
	}

	@Override
	public S3FileSystem getFileSystem(URI uri) {
		validateUri(uri);
		String key = this.getFileSystemKey(uri);
		if (fileSystems.containsKey(key)) {
            return fileSystems.get(key);
        }
        else{
            throw new FileSystemNotFoundException(
                    String.format("S3 filesystem not yet created. Use newFileSystem() instead"));
        }
	}

	private S3Path toS3Path(Path path) {
		Preconditions.checkArgument(path instanceof S3Path, "path must be an instance of %s", S3Path.class.getName());
		return (S3Path) path;
	}

	/**
	 * Deviation from spec: throws FileSystemNotFoundException if FileSystem
	 * hasn't yet been initialized. Call newFileSystem() first.
	 * Need credentials. Maybe set credentials after? how?
     * TODO: we can create a new one if the credentials are present by:
     * s3://access-key:secret-key@endpoint.com/
	 */
	@Override
	public Path getPath(URI uri) {
		FileSystem fileSystem = getFileSystem(uri);
		/**
		 * TODO: set as a list. one s3FileSystem by region
		 */
		return fileSystem.getPath(uri.getPath());
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
		final S3Path s3Path = toS3Path(dir);
		return new DirectoryStream<Path>() {
			@Override
			public void close() throws IOException {
				// nothing to do here
			}

			@Override
			public Iterator<Path> iterator() {
				return new S3Iterator(s3Path);
			}
		};
	}

	@Override
	public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        S3Path s3Path = toS3Path(path);
        String key = s3Path.getKey();

        Preconditions.checkArgument(options.length == 0, "OpenOptions not yet supported: %s", ImmutableList.copyOf(options)); // TODO
        Preconditions.checkArgument(!key.equals(""), "cannot create InputStream for root directory: %s", path);

        try {
            S3Object object = s3Path.getFileSystem().getClient().getObject(s3Path.getFileStore().name(), key);
            InputStream res = object.getObjectContent();

            if (res == null)
                throw new IOException(String.format("The specified path is a directory: %s", path));

            return res;
        }
        catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404)
                throw new NoSuchFileException(path.toString());
            // otherwise throws a generic IO exception
            throw new IOException(String.format("Cannot access file: %s", path), e);
        }
	}

    @Override
    public OutputStream newOutputStream(final Path path, final OpenOption... options) throws IOException {
        S3Path s3Path = toS3Path(path);

        // validate options
        if (options.length > 0) {
            Set<OpenOption> opts = new LinkedHashSet<>(Arrays.asList(options));

            // cannot handle APPEND here -> use newByteChannel() implementation
            if (opts.contains(StandardOpenOption.APPEND)) {
                return super.newOutputStream(path, options);
            }

            if (opts.contains(StandardOpenOption.READ)) {
                throw new IllegalArgumentException("READ not allowed");
            }

            boolean create = opts.remove(StandardOpenOption.CREATE);
            boolean createNew = opts.remove(StandardOpenOption.CREATE_NEW);
            boolean truncateExisting = opts.remove(StandardOpenOption.TRUNCATE_EXISTING);

            // remove irrelevant/ignored options
            opts.remove(StandardOpenOption.WRITE);
            opts.remove(StandardOpenOption.SPARSE);

            if (!opts.isEmpty()) {
                throw new UnsupportedOperationException(opts.iterator().next() + " not supported");
            }

            if (!(create && truncateExisting)) {
                if (s3Path.getFileSystem().provider().exists(s3Path)) {
                    if (createNew || !truncateExisting) {
                        throw new FileAlreadyExistsException(path.toString());
                    }
                } else {
                    if (!createNew && !create) {
                        throw new NoSuchFileException(path.toString());
                    }
                }
            }
        }

        return new S3OutputStream(s3Path.getFileSystem().getClient(), s3Path.toS3ObjectId());
    }

    @Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        S3Path s3Path = toS3Path(path);
        return new S3SeekableByteChannel(s3Path, options);
	}

	/**
	 * Deviations from spec: Does not perform atomic check-and-create. Since a
	 * directory is just an S3 object, all directories in the hierarchy are
	 * created or it already existed.
	 */
	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		S3Path s3Path = toS3Path(dir);
		Preconditions.checkArgument(attrs.length == 0, "attrs not yet supported: %s", ImmutableList.copyOf(attrs)); // TODO
        if (exists(s3Path))
            throw new FileAlreadyExistsException(format("target already exists: %s", s3Path));
        // create bucket if necesary
        Bucket bucket = s3Path.getFileStore().getBucket();
        String bucketName = s3Path.getFileStore().name();
        if (bucket == null){
            s3Path.getFileSystem().getClient().createBucket(bucketName);
        }
        // create the object as directory
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        s3Path.getFileSystem().getClient().putObject(bucketName, s3Path.getKey() + "/", new ByteArrayInputStream(new byte[0]), metadata);
	}

	@Override
	public void delete(Path path) throws IOException {
        S3Path s3Path = toS3Path(path);
        if (Files.notExists(s3Path))
            throw new NoSuchFileException("the path: " + this + " not exists");
        if (Files.isDirectory(s3Path) && Files.newDirectoryStream(s3Path).iterator().hasNext())
            throw new DirectoryNotEmptyException("the path: " + this + " is a directory and is not empty");

        String key = s3Path.getKey();
        String bucketName = s3Path.getFileStore().name();
        s3Path.getFileSystem().getClient().deleteObject(bucketName, key);
        // we delete the two objects (sometimes exists the key '/' and sometimes not)
        s3Path.getFileSystem().getClient().deleteObject(bucketName, key + "/");
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
		if (isSameFile(source, target))
			return;

		S3Path s3Source = toS3Path(source);
		S3Path s3Target = toS3Path(target);
		// TODO: implements support for copying directories

		Preconditions.checkArgument(!Files.isDirectory(source), "copying directories is not yet supported: %s", source);
		Preconditions.checkArgument(!Files.isDirectory(target), "copying directories is not yet supported: %s", target);
		
		ImmutableSet<CopyOption> actualOptions = ImmutableSet.copyOf(options);
		verifySupportedOptions(EnumSet.of(StandardCopyOption.REPLACE_EXISTING), actualOptions);

		if (exists(s3Target) && !actualOptions.contains(StandardCopyOption.REPLACE_EXISTING)){
            throw new FileAlreadyExistsException(format("target already exists: %s", target));
        }

        String bucketNameOrigin = s3Source.getFileStore().name();
        String keySource = s3Source.getKey();
        String bucketNameTarget = s3Target.getFileStore().name();
        String keyTarget = s3Target.getKey();
        s3Source.getFileSystem()
                .getClient().copyObject(
                    bucketNameOrigin,
                    keySource,
                    bucketNameTarget,
                    keyTarget);
	}

	@Override
	public void move(Path source, Path target, CopyOption... options) throws IOException {
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
		S3Path s3Path = toS3Path(path);
		Preconditions.checkArgument(s3Path.isAbsolute(), "path must be absolute: %s", s3Path);
        if (modes.length == 0) {
            if (exists(s3Path))
                return;
            throw new NoSuchFileException(toString());
        }

        String key = s3Utils.getS3ObjectSummary(s3Path).getKey();
        S3AccessControlList accessControlList =
                new S3AccessControlList(s3Path.getFileStore().name(), key, s3Path.getFileSystem().getClient().getObjectAcl(s3Path.getFileStore().name(), key), s3Path.getFileStore().getOwner());

        accessControlList.checkAccess(modes);
	}



	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
		S3Path s3Path = toS3Path(path);
        if (type == BasicFileAttributes.class) {
            return type.cast(s3Utils.getS3FileAttributes(s3Path));
        }
        throw new UnsupportedOperationException(format("only %s supported", BasicFileAttributes.class));
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}

	// ~~

	/**
	 * Create the fileSystem
	 * @param uri URI
	 * @param props Properties
	 * @return S3FileSystem never null
	 */
	protected S3FileSystem createFileSystem(URI uri, Properties props) {
		return new S3FileSystem(this, getFileSystemKey(uri, props), getAmazonS3(uri, props), uri.getHost());
	}

	protected AmazonS3 getAmazonS3(URI uri, Properties props) {
		return getAmazonS3Factory(props).getAmazonS3(uri, props);
	}

	protected AmazonS3Factory getAmazonS3Factory(Properties props) {
		if (props.containsKey(AMAZON_S3_FACTORY_CLASS)) {
			String amazonS3FactoryClass = props.getProperty(AMAZON_S3_FACTORY_CLASS);
			try {
				return (AmazonS3Factory) Class.forName(amazonS3FactoryClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | ClassCastException e) {
				throw new S3FileSystemConfigurationException("Configuration problem, couldn't instantiate AmazonS3Factory (" + amazonS3FactoryClass + "): ", e);
			}
		}
		return new AmazonS3ClientFactory();
	}

	/**
	 * find /amazon.properties in the classpath
	 * @return Properties amazon.properties
	 */
	protected Properties loadAmazonProperties() {
		Properties props = new Properties();
		// http://www.javaworld.com/javaworld/javaqa/2003-06/01-qa-0606-load.html
		// http://www.javaworld.com/javaqa/2003-08/01-qa-0808-property.html
		try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("amazon.properties")) {
			if (in != null)
				props.load(in);
		} catch (IOException e) {
			// If amazon.properties can't be loaded that's ok. 
		}
		return props;
	}

	// ~~~

	private <T> void verifySupportedOptions(Set<? extends T> allowedOptions, Set<? extends T> actualOptions) {
		Sets.SetView<? extends T> unsupported = difference(actualOptions, allowedOptions);
		Preconditions.checkArgument(unsupported.isEmpty(), "the following options are not supported: %s", unsupported);
	}

	/**
	 * check that the paths exists or not
	 * @param path S3Path
	 * @return true if exists
	 */
	boolean exists(S3Path path) {
        S3Path s3Path = toS3Path(path);
        try {
            s3Utils.getS3ObjectSummary(s3Path);
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
	}

	public void close(S3FileSystem fileSystem) {
		if(fileSystem.getKey() != null && fileSystems.containsKey(fileSystem.getKey()))
			fileSystems.remove(fileSystem.getKey());
	}

	public boolean isOpen(S3FileSystem s3FileSystem) {
		return fileSystems.containsKey(s3FileSystem.getKey());
	}

    /**
     * only 4 testing
     * @return
     */
	protected static ConcurrentMap<String, S3FileSystem> getFilesystems() {
		return fileSystems;
	}
}