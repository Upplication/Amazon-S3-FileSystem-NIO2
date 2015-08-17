package com.upplication.s3fs;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class S3Path implements Path {

	public static final String PATH_SEPARATOR = "/";
	/**
	 * S3FileStore which represents the Bucket this path resides in.
	 */
	private final S3FileStore fileStore;
	/**
	 * Parts without bucket name.
	 */
	private final List<String> parts;
	/**
	 * actual filesystem
	 */
	private S3FileSystem fileSystem;

	private S3FileAttributes fileAttributes;

	/**
	 * path must be a string of the form "/{bucket}", "/{bucket}/{key}" or just
	 * "{key}".
	 * Examples:
	 * <ul>
	 *  <li>"/{bucket}//{value}" good, empty key paths are ignored </li>
	 * <li> "//{key}" error, missing bucket</li>
	 * <li> "/" error, missing bucket </li>
	 * </ul>
	 *
	 */
	public S3Path(S3FileSystem fileSystem, String path) {
		this(fileSystem, path, "");
	}

	/**
	 * Build an S3Path from path segments. '/' are stripped from each segment.
	 * @param first should be start with a '/' and is the bucket name
	 * @param more directories and files
	 */
	public S3Path(S3FileSystem fileSystem, String first, String... more) {
		String bucket = null;
		List<String> pathParts = Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(first));

		if (first.endsWith(PATH_SEPARATOR))
			pathParts.remove(pathParts.size() - 1);

		if (first.startsWith(PATH_SEPARATOR)) { // absolute path
			pathParts = pathParts.subList(1, pathParts.size());
			Preconditions.checkArgument(pathParts.size() >= 1, "path must start with bucket name");
			Preconditions.checkArgument(!pathParts.get(0).isEmpty(), "bucket name must be not empty");
			bucket = pathParts.get(0);
			pathParts = pathParts.subList(1, pathParts.size());
		}

		List<String> moreSplitted = Lists.newArrayList();
		for (String part : more)
			moreSplitted.addAll(Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(part)));

		pathParts.addAll(moreSplitted);
		if (bucket != null)
			this.fileStore = new S3FileStore(fileSystem, bucket);
		else
			this.fileStore = null;
		this.parts = KeyParts.parse(pathParts);
		this.fileSystem = fileSystem;
	}

	S3Path(S3FileSystem fileSystem, S3FileStore fileStore, Iterable<String> keys) {
		this.fileStore = fileStore;
		this.parts = KeyParts.parse(keys);
		this.fileSystem = fileSystem;
	}

	S3Path(S3FileSystem fileSystem, S3FileStore fileStore, String... keys) {
		this.fileStore = fileStore;
		this.parts = KeyParts.parse(keys);
		this.fileSystem = fileSystem;
	}

	public S3FileStore getFileStore() {
		return fileStore;
	}

	/**
	 * key for amazon without final slash.
	 * <b>note:</b> the final slash need to be added to save a directory (Amazon s3 spec)
	 */
	public String getKey() {
		return fileSystem.parts2Key(parts);
	}

	@Override
	public S3FileSystem getFileSystem() {
		return this.fileSystem;
	}

	@Override
	public boolean isAbsolute() {
		return fileStore != null;
	}

	@Override
	public Path getRoot() {
		if (isAbsolute()) {
			return new S3Path(fileSystem, fileStore, ImmutableList.<String> of());
		}

		return null;
	}

	@Override
	public Path getFileName() {
		if (!parts.isEmpty())
			return new S3Path(fileSystem, null, parts.subList(parts.size() - 1, parts.size()));
		return new S3Path(fileSystem, (S3FileStore) null, fileStore != null ? fileStore.name() : null); // bucket dont have fileName
	}

	@Override
	public Path getParent() {
		// bucket is not present in the parts
		if (parts.isEmpty()) {
			return null;
		}

		if (parts.size() == 1 && fileStore == null) {
			return null;
		}

		return new S3Path(fileSystem, fileStore, parts.subList(0, parts.size() - 1));
	}

	@Override
	public int getNameCount() {
		return parts.size();
	}

	@Override
	public Path getName(int index) {
		return new S3Path(fileSystem, null, parts.subList(index, index + 1));
	}

	@Override
	public Path subpath(int beginIndex, int endIndex) {
		return new S3Path(fileSystem, null, parts.subList(beginIndex, endIndex));
	}

	@Override
	public boolean startsWith(Path other) {

		if (other.getNameCount() > this.getNameCount()) {
			return false;
		}

		if (!(other instanceof S3Path)) {
			return false;
		}

		S3Path path = (S3Path) other;

		if (path.parts.size() == 0 && path.fileStore == null && (this.parts.size() != 0 || this.fileStore != null)) {
			return false;
		}

		if ((path.getFileStore() != null && !path.getFileStore().equals(this.getFileStore())) || (path.getFileStore() == null && this.getFileStore() != null)) {
			return false;
		}

		for (int i = 0; i < path.parts.size(); i++) {
			if (!path.parts.get(i).equals(this.parts.get(i))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean startsWith(String path) {
		S3Path other = new S3Path(this.fileSystem, path);
		return this.startsWith(other);
	}

	@Override
	public boolean endsWith(Path other) {
		if (other.getNameCount() > this.getNameCount()) {
			return false;
		}
		// empty
		if (other.getNameCount() == 0 && this.getNameCount() != 0) {
			return false;
		}

		if (!(other instanceof S3Path)) {
			return false;
		}

		S3Path path = (S3Path) other;

		if ((path.getFileStore() != null && !path.getFileStore().equals(this.getFileStore())) || (path.getFileStore() != null && this.getFileStore() == null)) {
			return false;
		}

		// check subkeys

		int i = path.parts.size() - 1;
		int j = this.parts.size() - 1;
		for (; i >= 0 && j >= 0;) {

			if (!path.parts.get(i).equals(this.parts.get(j))) {
				return false;
			}
			i--;
			j--;
		}
		return true;
	}

	@Override
	public boolean endsWith(String other) {
		return this.endsWith(new S3Path(this.fileSystem, other));
	}

	@Override
	public Path normalize() {
		return this;
	}

	@Override
	public Path resolve(Path other) {
		if (other.isAbsolute()) {
			Preconditions.checkArgument(other instanceof S3Path, "other must be an instance of %s", S3Path.class.getName());
			return other;
		}

		ImmutableList.Builder<String> builder = ImmutableList.builder();
		for (int i = 0; i < other.getNameCount(); i++)
			builder.add(other.getName(i).toString());
		ImmutableList<String> otherParts = builder.build();
		if (otherParts.isEmpty()) // other is relative and empty
			return this;

		return new S3Path(fileSystem, fileStore, concat(parts, otherParts));
	}

	@Override
	public Path resolve(String other) {
		return resolve(new S3Path(this.getFileSystem(), other));
	}

	@Override
	public Path resolveSibling(Path other) {
		Preconditions.checkArgument(other instanceof S3Path, "other must be an instance of %s", S3Path.class.getName());

		S3Path s3Path = (S3Path) other;

		Path parent = getParent();

		if (parent == null || s3Path.isAbsolute()) {
			return s3Path;
		}

		if (s3Path.parts.isEmpty()) { // other is relative and empty
			return parent;
		}

		return new S3Path(fileSystem, fileStore, concat(parts.subList(0, parts.size() - 1), s3Path.parts));
	}

	@Override
	public Path resolveSibling(String other) {
		return resolveSibling(new S3Path(this.getFileSystem(), other));
	}

	@Override
	public Path relativize(Path other) {
		Preconditions.checkArgument(other instanceof S3Path, "other must be an instance of %s", S3Path.class.getName());
		S3Path s3Path = (S3Path) other;

		if (this.equals(other)) {
			return new S3Path(this.getFileSystem(), "");
		}

		Preconditions.checkArgument(isAbsolute(), "Path is already relative: %s", this);
		Preconditions.checkArgument(s3Path.isAbsolute(), "Cannot relativize against a relative path: %s", s3Path);
		Preconditions.checkArgument(fileStore.equals(s3Path.getFileStore()), "Cannot relativize paths with different buckets: '%s', '%s'", this, other);
		Preconditions.checkArgument(parts.size() <= s3Path.parts.size(), "Cannot relativize against a parent path: '%s', '%s'", this, other);

		int startPart = 0;
		for (int i = 0; i < this.parts.size(); i++)
			if (this.parts.get(i).equals(s3Path.parts.get(i)))
				startPart++;
		return new S3Path(fileSystem, null, s3Path.parts.subList(startPart, s3Path.parts.size()));
	}

	@Override
	public URI toUri() {
		if (fileStore == null)
			return null;
		
		StringBuilder builder = new StringBuilder();
		builder.append("s3://");
        builder.append(fileSystem.getKey());
		builder.append("/");
		builder.append(fileStore.name());
		builder.append(PATH_SEPARATOR);
		builder.append(Joiner.on(PATH_SEPARATOR).join(parts));
		return URI.create(builder.toString());
	}

	@Override
	public Path toAbsolutePath() {
		if (isAbsolute()) {
			return this;
		}

		throw new IllegalStateException(format("Relative path cannot be made absolute: %s", this));
	}

	@Override
	public Path toRealPath(LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public File toFile() {
		throw new UnsupportedOperationException();
	}

	@Override
	public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Path> iterator() {
		ImmutableList.Builder<Path> builder = ImmutableList.builder();

		for (String part : parts) {
			builder.add(new S3Path(fileSystem, null, ImmutableList.of(part)));
		}

		return builder.build().iterator();
	}

	@Override
	public int compareTo(Path other) {
		return toString().compareTo(other.toString());
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		if (isAbsolute()) {
			builder.append(PATH_SEPARATOR);
			builder.append(fileStore.name());
			builder.append(PATH_SEPARATOR);
		}
		List<String> parts2 = parts;
		for (Iterator<String> iterator = parts2.iterator(); iterator.hasNext();) {
			String part = iterator.next();
			builder.append(part);
			if(iterator.hasNext())
				builder.append(PATH_SEPARATOR);
		}
		return builder.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		S3Path paths = (S3Path) o;
		if (fileStore != null ? !fileStore.equals(paths.fileStore) : paths.fileStore != null)
			return false;
		if (!parts.equals(paths.parts))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = fileStore != null ? fileStore.name().hashCode() : 0;
		result = 31 * result + parts.hashCode();
		return result;
	}

	// ~ helpers methods

	private static Function<String, String> strip(final String... strs) {
		return new Function<String, String>() {
			@Override
			public String apply(String input) {
				String res = input;
				if(res != null)
					for (String str : strs)
						res = res.replace(str, "");
				return res;
			}
		};
	}

	private static Predicate<String> notEmpty() {
		return new Predicate<String>() {
			@Override
			public boolean apply(@Nullable String input) {
				return input != null && !input.isEmpty();
			}
		};
	}

	public boolean hasFileAttributes() {
		return fileAttributes != null;
	}

	public S3FileAttributes getFileAttributes() {
		return fileAttributes;
	}

	public void setFileAttributes(S3FileAttributes fileAttributes) {
		this.fileAttributes = fileAttributes;
	}

	/*
	 * delete redundant "/" and empty parts
	 */
	private abstract static class KeyParts {
		private static ImmutableList<String> parse(String[] parts) {
			return ImmutableList.copyOf(filter(transform(Arrays.asList(parts), strip("/")), notEmpty()));
		}

		private static ImmutableList<String> parse(List<String> parts) {
			return ImmutableList.copyOf(filter(transform(parts, strip("/")), notEmpty()));
		}

		private static ImmutableList<String> parse(Iterable<String> parts) {
			return ImmutableList.copyOf(filter(transform(parts, strip("/")), notEmpty()));
		}
	}

}