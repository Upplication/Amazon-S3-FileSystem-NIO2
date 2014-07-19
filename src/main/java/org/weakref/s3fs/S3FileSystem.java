package org.weakref.s3fs;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

import com.amazonaws.services.s3.AmazonS3Client;
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
		if (more.length == 0) {
			return new S3Path(this, first);
		}

		return new S3Path(this, first, more);
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
	 * @see http://docs.aws.amazon.com/general/latest/gr/rande.html
	 * @return string
	 */
	public String getEndpoint() {
		return endpoint;
	}
}
