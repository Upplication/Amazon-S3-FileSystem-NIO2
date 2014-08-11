package com.upplication.s3fs;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Set;

import com.amazonaws.services.s3.model.Bucket;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class S3FileSystem extends FileSystem {
	
	private final S3FileSystemProvider provider;
	private final AmazonS3Client client;
	private final String endpoint;

	public S3FileSystem(S3FileSystemProvider provider, AmazonS3Client client,
			String endpoint) {
		this.provider = provider;
		this.client = client;
		this.endpoint = endpoint;
	}

	@Override
	public FileSystemProvider provider() {
		return provider;
	}

	@Override
	public void close() throws IOException {
		this.provider.fileSystem.compareAndSet(this, null);
	}

	@Override
	public boolean isOpen() {
		return this.provider.fileSystem.get() != null;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public String getSeparator() {
		return S3Path.PATH_SEPARATOR;
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		ImmutableList.Builder<Path> builder = ImmutableList.builder();

		for (Bucket bucket : client.listBuckets()) {
			builder.add(new S3Path(this, bucket.getName()));
		}

		return builder.build();
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		return ImmutableList.of();
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		return ImmutableSet.of("basic");
	}

	@Override
	public Path getPath(String first, String... more) {
	    ArrayList<String> parts = new ArrayList<String>();
	    String[] firstParts = first.split(S3Path.PATH_SEPARATOR);
	    for (int i=0; i < firstParts.length ; i++) {
	        String part = firstParts[i];
	        if (part != null && ! part.isEmpty()) parts.add(part);
	        else if ( i != 0 ) throw new InvalidPathException(part, "Null or empty 'first' path element", i);
	    }
	    for (int i=0; i < more.length ; i++) {
	        String part = more[i];
	        if (part != null && ! part.isEmpty()) parts.add(part);
	        else throw new InvalidPathException(part, "Null or empty 'more' path element", i);
	    }
	    first = parts.remove(0);

		if (parts.size() == 0) {
			return new S3Path(this, first);
		}

		return new S3Path(this, first, parts.toArray(new String[0]));
	}

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		throw new UnsupportedOperationException();
	}

	@Override
	public WatchService newWatchService() throws IOException {
		throw new UnsupportedOperationException();
	}

	public AmazonS3Client getClient() {
		return client;
	}

	/**
	 * get the endpoint associated with this fileSystem.
	 * 
	 * @see <a href="http://docs.aws.amazon.com/general/latest/gr/rande.html">http://docs.aws.amazon.com/general/latest/gr/rande.html</a>
	 * @return string
	 */
	public String getEndpoint() {
		return endpoint;
	}
}
