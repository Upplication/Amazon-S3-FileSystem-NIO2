package com.upplication.s3fs;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class FileSystemProviderIT {
	S3FileSystemProvider provider;
	
	@Before
	public void setup() throws IOException{
		try {
			FileSystems.getFileSystem(URI.create("s3:///")).close();
		}
		catch(FileSystemNotFoundException e){}
		
		
		provider = spy(new S3FileSystemProvider());
	}
	
	@Test
	public void createAuthenticatedByProperties() throws IOException{

		URI uri = URI.create("s3:///");
		
		FileSystem fileSystem = provider.newFileSystem(uri, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		
		verify(provider).createFileSystem(eq(uri), eq(buildFakeProps("access key for test", "secret key for test")));
	}
	
	
	@Test
	public void createsAuthenticatedByEnvOverridesProps() throws IOException {
		
		Map<String, ?> env = buildFakeEnv();
		URI uri = URI.create("s3:///");
		
		FileSystem fileSystem = provider.newFileSystem(uri, env);

		assertNotNull(fileSystem);
		verify(provider).createFileSystem(eq(uri), eq(buildFakeProps((String) env.get(S3FileSystemProvider.ACCESS_KEY), (String) env.get(S3FileSystemProvider.SECRET_KEY))));
	}

	@Test
	public void createsAnonymousNotPossible() throws IOException {
		URI uri = URI.create("s3:///");
		FileSystem fileSystem = provider.newFileSystem(uri, ImmutableMap.<String, Object> of());

		assertNotNull(fileSystem);
		verify(provider).createFileSystem(eq(uri), eq(buildFakeProps("access key for test", "secret key for test")));
	}
	
	private Map<String, ?> buildFakeEnv(){
		return ImmutableMap.<String, Object> builder()
				.put(S3FileSystemProvider.ACCESS_KEY, "access key")
				.put(S3FileSystemProvider.SECRET_KEY, "secret key").build();
	}

	private Properties buildFakeProps(String access_key, String secret_key) {
		Properties props = new Properties();
		props.setProperty(S3FileSystemProvider.ACCESS_KEY, access_key);
		props.setProperty(S3FileSystemProvider.SECRET_KEY, secret_key);
		props.setProperty("bucket_name","/your-bucket-name for test");
		return props;
	}
	

}
