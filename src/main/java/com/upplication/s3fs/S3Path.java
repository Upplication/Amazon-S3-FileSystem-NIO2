package com.upplication.s3fs;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.upplication.s3fs.util.S3KeyHelper;

public class S3Path implements Path {
	
	public static final String PATH_SEPARATOR = "/";
	/**
	 * bucket name
	 */
	private final String bucket;
	/**
	 * Parts without bucket name.
	 */
	private final List<String> parts;
	/**
	 * actual filesystem
	 */
	private S3FileSystem fileSystem;

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
     * @param first should be star with a '/' and the first element is the bucket
     * @param more directories and files
     */
	public S3Path(S3FileSystem fileSystem, String first, String ... more) {
        String bucket = null;
        List<String> parts = Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(first));

        if (first.endsWith(PATH_SEPARATOR)) {
            parts.remove(parts.size()-1);
        }

        if (first.startsWith(PATH_SEPARATOR)) { // absolute path
            Preconditions.checkArgument(parts.size() >= 1, "path must start with bucket name");
            Preconditions.checkArgument(!parts.get(1).isEmpty(), "bucket name must be not empty");

            bucket = parts.get(1);

            if (!parts.isEmpty()) {
                parts = parts.subList(2, parts.size());
            }
        }

        if (bucket != null) {
            bucket = bucket.replace("/", "");
        }

        List<String> moreSplitted = Lists.newArrayList();

        for (String part : more){
            moreSplitted.addAll(Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(part)));
        }

        parts.addAll(moreSplitted);


		this.bucket = bucket;
		this.parts = KeyParts.parse(parts);
		this.fileSystem = fileSystem;
	}

    private S3Path(S3FileSystem fileSystem, String bucket,
                   Iterable<String> keys){
        this.bucket = bucket;
        this.parts = KeyParts.parse(keys);
        this.fileSystem = fileSystem;
    }

	
	public String getBucket() {
		return bucket;
	}
	/**
	 * key for amazon without final slash.
	 * <b>note:</b> the final slash need to be added to save a directory (Amazon s3 spec)
	 */
	public String getKey() {
		if (parts.isEmpty()) {
			return "";
		}
		return S3KeyHelper.getKey(parts);
	}

	@Override
	public S3FileSystem getFileSystem() {
		return this.fileSystem;
	}

	@Override
	public boolean isAbsolute() {
		return bucket != null;
	}

	@Override
	public Path getRoot() {
		if (isAbsolute()) {
			return new S3Path(fileSystem, bucket, ImmutableList.<String> of());
		}

		return null;
	}

	@Override
	public Path getFileName() {
		if (!parts.isEmpty()) {
			return new S3Path(fileSystem, null, parts.subList(parts.size() - 1,
					parts.size()));
		}
        else {
            // bucket dont have fileName
            return null;
        }
	}

	@Override
	public Path getParent() {
		// bucket is not present in the parts
		if (parts.isEmpty()) {
			return null;
		}

		if (parts.size() == 1 && (bucket == null || bucket.isEmpty())){
			return null;
		}

		return new S3Path(fileSystem, bucket,
				parts.subList(0, parts.size() - 1));
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
		
		if (other.getNameCount() > this.getNameCount()){
			return false;
		}
		
		if (!(other instanceof S3Path)){
			return false;
		}
		
		S3Path path = (S3Path) other;

		if (path.parts.size() == 0 && path.bucket == null &&
				(this.parts.size() != 0 || this.bucket != null)){
			return false;
		}

		if ((path.getBucket() != null && !path.getBucket().equals(this.getBucket())) ||
				(path.getBucket() == null && this.getBucket() != null)){
			return false;
		}

		for (int i = 0; i < path.parts.size() ; i++){
			if (!path.parts.get(i).equals(this.parts.get(i))){
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
		if (other.getNameCount() > this.getNameCount()){
			return false;
		}
		// empty
		if (other.getNameCount() == 0 && 
				this.getNameCount() != 0){
			return false;
		}
		
		if (!(other instanceof S3Path)){
			return false;
		}
		
		S3Path path = (S3Path) other;

		if ((path.getBucket() != null && !path.getBucket().equals(this.getBucket())) ||
				(path.getBucket() != null && this.getBucket() == null)){
			return false;
		}
		
		// check subkeys
		
		int i = path.parts.size() - 1;
		int j = this.parts.size() - 1;
		for (; i >= 0 && j >= 0 ;){
			
			if (!path.parts.get(i).equals(this.parts.get(j))){
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
		Preconditions.checkArgument(other instanceof S3Path,
				"other must be an instance of %s", S3Path.class.getName());

		S3Path s3Path = (S3Path) other;

		if (s3Path.isAbsolute()) {
			return s3Path;
		}

		if (s3Path.parts.isEmpty()) { // other is relative and empty
			return this;
		}

		return new S3Path(fileSystem, bucket, concat(parts, s3Path.parts));
	}

	@Override
	public Path resolve(String other) {
		return resolve(new S3Path(this.getFileSystem(), other));
	}

	@Override
	public Path resolveSibling(Path other) {
		Preconditions.checkArgument(other instanceof S3Path,
				"other must be an instance of %s", S3Path.class.getName());

		S3Path s3Path = (S3Path) other;

		Path parent = getParent();

		if (parent == null || s3Path.isAbsolute()) {
			return s3Path;
		}

		if (s3Path.parts.isEmpty()) { // other is relative and empty
			return parent;
		}

		return new S3Path(fileSystem, bucket, concat(
				parts.subList(0, parts.size() - 1), s3Path.parts));
	}

	@Override
	public Path resolveSibling(String other) {
		return resolveSibling(new S3Path(this.getFileSystem(), other));
	}

	@Override
	public Path relativize(Path other) {
		Preconditions.checkArgument(other instanceof S3Path,
				"other must be an instance of %s", S3Path.class.getName());
		S3Path s3Path = (S3Path) other;

		if (this.equals(other)) {
			return new S3Path(this.getFileSystem(), "");
		}

		Preconditions.checkArgument(isAbsolute(),
				"Path is already relative: %s", this);
		Preconditions.checkArgument(s3Path.isAbsolute(),
				"Cannot relativize against a relative path: %s", s3Path);
		Preconditions.checkArgument(bucket.equals(s3Path.getBucket()),
				"Cannot relativize paths with different buckets: '%s', '%s'",
				this, other);
		
		Preconditions.checkArgument(parts.size() <= s3Path.parts.size(),
				"Cannot relativize against a parent path: '%s', '%s'",
				this, other);
		
		
		int startPart = 0;
		for (int i = 0; i <this.parts.size() ; i++){
			if (this.parts.get(i).equals(s3Path.parts.get(i))){
				startPart++;
			}
		}
		
		List<String> resultParts = new ArrayList<>();
		for (int i = startPart; i < s3Path.parts.size(); i++){
			resultParts.add(s3Path.parts.get(i));
		}

		return new S3Path(fileSystem, null, resultParts);
	}

	@Override
	public URI toUri() {
		StringBuilder builder = new StringBuilder();
		builder.append("s3://");
		if (fileSystem.getEndpoint() != null) {
			builder.append(fileSystem.getEndpoint());
		}
		builder.append("/");
		builder.append(bucket);
		builder.append(PATH_SEPARATOR);
		builder.append(Joiner.on(PATH_SEPARATOR).join(parts));
		return URI.create(builder.toString());
	}

	@Override
	public Path toAbsolutePath() {
		if (isAbsolute()) {
			return this;
		}

		throw new IllegalStateException(format(
				"Relative path cannot be made absolute: %s", this));
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
	public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events,
			WatchEvent.Modifier... modifiers) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Path> iterator() {
		ImmutableList.Builder<Path> builder = ImmutableList.builder();

		for (Iterator<String> iterator = parts.iterator(); iterator.hasNext();) {
			String part = iterator.next();
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
			builder.append(bucket);
			builder.append(PATH_SEPARATOR);
		}

		builder.append(Joiner.on(PATH_SEPARATOR).join(parts));

		return builder.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		S3Path paths = (S3Path) o;

		if (bucket != null ? !bucket.equals(paths.bucket)
				: paths.bucket != null) {
			return false;
		}
		if (!parts.equals(paths.parts)) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = bucket != null ? bucket.hashCode() : 0;
		result = 31 * result + parts.hashCode();
		return result;
	}
	
	// ~ helpers methods

	private static Function<String, String> strip(final String ... strs) {
		return new Function<String, String>() {
			public String apply(String input) {
				String res = input;
				for (String str : strs) {
					res = res.replace(str, "");
				}
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
	/*
	 * delete redundant "/" and empty parts
	 */
	private abstract static class KeyParts{
		
		private static ImmutableList<String> parse(List<String> parts) {
			return ImmutableList.copyOf(filter(transform(parts, strip("/")), notEmpty()));
		}
		
		private static ImmutableList<String> parse(Iterable<String> parts) {
			return ImmutableList.copyOf(filter(transform(parts, strip("/")), notEmpty()));
		}
	}
}
