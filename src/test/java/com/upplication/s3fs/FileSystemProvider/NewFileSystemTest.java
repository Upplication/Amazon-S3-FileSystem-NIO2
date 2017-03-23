package com.upplication.s3fs.FileSystemProvider;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS;
import static com.upplication.s3fs.S3FileSystemProvider.CHARSET_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.amazonaws.services.s3.internal.Constants;
import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemConfigurationException;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.S3EndpointConstant;

public class NewFileSystemTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() {
        s3fsProvider = spy(new S3FileSystemProvider());
        doReturn(false).when(s3fsProvider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
        doReturn(new Properties()).when(s3fsProvider).loadAmazonProperties();
    }

    @Test(expected = S3FileSystemConfigurationException.class)
    public void missconfigure() {
        Properties props = new Properties();
        props.setProperty(AMAZON_S3_FACTORY_CLASS, "com.upplication.s3fs.util.BrokenAmazonS3Factory");
        s3fsProvider.createFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);
    }

    @Test
    public void newS3FileSystemWithEmptyHostAndUserInfo() throws IOException {
        FileSystem s3fs = s3fsProvider.newFileSystem(URI.create("s3:///bucket/file"), ImmutableMap.<String, Object>of());
        assertEquals(Constants.S3_HOSTNAME, ((S3FileSystem) s3fs).getKey());
    }

    @Test
    public void newS3FileSystemWithEmptyHost() throws IOException {
        FileSystem s3fs = s3fsProvider.newFileSystem(URI.create("s3://access-key:secret-key@/bucket/file"), ImmutableMap.<String, Object>of());
        assertEquals("access-key@" + Constants.S3_HOSTNAME, ((S3FileSystem) s3fs).getKey());
    }

    @Test
	public void newS3FileSystemWithCustomHost() {
		FileSystem s3fs = s3fsProvider.newFileSystem(URI.create("s3://access-key:secret-key@my.ceph.storage"), ImmutableMap.<String, Object> of());
		assertEquals("access-key@my.ceph.storage", ((S3FileSystem) s3fs).getKey());
	}

	@Test
	public void newS3FileSystemWithCustomHostAndBucket() {
		FileSystem s3fs = s3fsProvider.newFileSystem(URI.create("s3://access-key:secret-key@my.ceph.storage/bucket"), ImmutableMap.<String, Object> of());
		assertEquals("access-key@my.ceph.storage", ((S3FileSystem) s3fs).getKey());
	}

	@Test
    public void createsAuthenticatedByEnv() {
        Map<String, ?> env = buildFakeEnv();
        FileSystem fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);
        assertNotNull(fileSystem);
        verify(s3fsProvider).createFileSystem(eq(S3EndpointConstant.S3_GLOBAL_URI_TEST), eq(buildFakeProps((String) env.get(ACCESS_KEY), (String) env.get(SECRET_KEY))));
    }

    @Test
    public void setEncodingByProperties() {
        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "better secret key");
        props.setProperty(ACCESS_KEY, "better access key");
        props.setProperty(CHARSET_KEY, "UTF-8");
        doReturn(props).when(s3fsProvider).loadAmazonProperties();
        URI uri = S3EndpointConstant.S3_GLOBAL_URI_TEST;

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);

        verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps("better access key", "better secret key", "UTF-8")));
    }

    @Test
    public void createAuthenticatedByProperties() {
        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "better secret key");
        props.setProperty(ACCESS_KEY, "better access key");
        doReturn(props).when(s3fsProvider).loadAmazonProperties();
        URI uri = S3EndpointConstant.S3_GLOBAL_URI_TEST;

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);

        verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps("better access key", "better secret key")));
    }

    @Test
    public void createAuthenticatedBySystemEnvironment() {

        final String accessKey = "better-access-key";
        final String secretKey = "better-secret-key";

        doReturn(accessKey).when(s3fsProvider).systemGetEnv(ACCESS_KEY);
        doReturn(secretKey).when(s3fsProvider).systemGetEnv(SECRET_KEY);
        doCallRealMethod().when(s3fsProvider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());

        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, ImmutableMap.<String, Object>of());

        verify(s3fsProvider).createFileSystem(eq(S3EndpointConstant.S3_GLOBAL_URI_TEST), argThat(new ArgumentMatcher<Properties>() {
            @Override
            public boolean matches(Object argument) {
                Properties called = (Properties) argument;
                assertEquals(accessKey, called.getProperty(ACCESS_KEY));
                assertEquals(secretKey, called.getProperty(SECRET_KEY));
                return true;
            }
        }));
    }

    @Test
    public void createsAnonymous() {
        URI uri = S3EndpointConstant.S3_GLOBAL_URI_TEST;
        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);
        verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps(null, null)));
    }

    @Test
    public void createWithDefaultEndpoint() {
        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "better secret key");
        props.setProperty(ACCESS_KEY, "better access key");
        props.setProperty(CHARSET_KEY, "UTF-8");
        doReturn(props).when(s3fsProvider).loadAmazonProperties();
        URI uri = URI.create("s3:///");

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);

        verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps("better access key", "better secret key", "UTF-8")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createWithOnlyAccessKey() {
        Properties props = new Properties();
        props.setProperty(ACCESS_KEY, "better access key");
        doReturn(props).when(s3fsProvider).loadAmazonProperties();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, ImmutableMap.<String, Object>of());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createWithOnlySecretKey() {
        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "better secret key");
        doReturn(props).when(s3fsProvider).loadAmazonProperties();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, ImmutableMap.<String, Object>of());
    }

    @Test(expected = FileSystemAlreadyExistsException.class)
    public void createFailsIfAlreadyCreated() {
        FileSystem fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, ImmutableMap.<String, Object>of());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createWithWrongEnv() {
        Map<String, Object> env = ImmutableMap.<String, Object>builder().put(ACCESS_KEY, 1234).put(SECRET_KEY, "secret key").build();
        FileSystem fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);
        assertNotNull(fileSystem);
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, ImmutableMap.<String, Object>of());
    }

    @Test
    public void getFileSystem() {
        FileSystem fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);
        fileSystem = s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);
        FileSystem other = s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        assertSame(fileSystem, other);
    }

    @Test
    public void getUnknownFileSystem() {
        FileSystem fileSystem = s3fsProvider.getFileSystem(URI.create("s3://endpoint20/bucket/path/to/file"), ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);
    }

    @Test
    public void closeFileSystemReturnNewFileSystem() throws IOException {
        S3FileSystemProvider provider = new S3FileSystemProvider();
        Map<String, ?> env = buildFakeEnv();
        FileSystem fileSystem = provider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);
        assertNotNull(fileSystem);
        fileSystem.close();
        FileSystem fileSystem2 = provider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);
        assertNotSame(fileSystem, fileSystem2);
    }

    @Test(expected = FileSystemAlreadyExistsException.class)
    public void createTwoFileSystemThrowError() {
        S3FileSystemProvider provider = new S3FileSystemProvider();
        Map<String, ?> env = buildFakeEnv();
        FileSystem fileSystem = provider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);
        assertNotNull(fileSystem);
        provider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);
    }


    private Map<String, ?> buildFakeEnv() {
        return ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access key").put(SECRET_KEY, "secret key").build();
    }

    private Properties buildFakeProps(String access_key, String secret_key, String encoding) {
        Properties props = buildFakeProps(access_key, secret_key);
        props.setProperty(CHARSET_KEY, encoding);
        return props;
    }


    private Properties buildFakeProps(String access_key, String secret_key) {
        Properties props = new Properties();
        props.setProperty(AMAZON_S3_FACTORY_CLASS, "com.upplication.s3fs.util.AmazonS3MockFactory");
        if (access_key != null)
            props.setProperty(ACCESS_KEY, access_key);
        if (secret_key != null)
            props.setProperty(SECRET_KEY, secret_key);
        return props;
    }
}