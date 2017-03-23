package com.upplication.s3fs.FileSystemProvider;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.S3FileSystemProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.ProviderNotFoundException; 
import java.lang.IllegalArgumentException;
import java.lang.SecurityException;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;


import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.Assert.*;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class NewFileSystemIT {

    private S3FileSystemProvider provider;

    @Before
    public void setup() throws IOException {
        System.clearProperty(S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS);
        System.clearProperty(ACCESS_KEY);
        System.clearProperty(SECRET_KEY);
        try {
            FileSystems.getFileSystem(S3_GLOBAL_URI_IT).close();
        } catch (FileSystemNotFoundException e) {
            // ignore this
        }
        provider = spy(new S3FileSystemProvider());
        doReturn(buildFakeProps()).when(provider).loadAmazonProperties();
        // dont override with system envs that we can have setted, like travis
        doReturn(false).when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
        doReturn(false).when(provider).overloadPropertiesWithSystemProps(any(Properties.class), anyString());
    }

    @Test
    public void createAuthenticatedByProperties() {

        URI uri = URI.create("s3://yadi/");

        FileSystem fileSystem = provider.newFileSystem(uri, null);
        assertNotNull(fileSystem);

        verify(provider).createFileSystem(eq(uri), eq(buildFakeProps()));
    }


    @Test
    public void createsAuthenticatedByEnvOverridesProps() {

        final Map<String, String> env = buildFakeEnv();
        provider.newFileSystem(S3_GLOBAL_URI_IT, env);

        verifyCreationWithParams(env.get(ACCESS_KEY), env.get(SECRET_KEY), S3_GLOBAL_URI_IT);
    }

    @Test
    public void createsAuthenticatedByEnvOverridesPropsNIO() {
        final Map<String, String> env = buildFakeEnv();
        try {
        	FileSystem fs = FileSystems.newFileSystem(S3_GLOBAL_URI_IT, env);
        	assertEquals( "Mismatch when retrieving file system.", fs, FileSystems.getFileSystem(S3_GLOBAL_URI_IT));
        } catch (Exception e) {
        	fail("File system creation failed");
        }
    }

    @Test
    public void createsAuthenticatedBySystemProps() {

        doCallRealMethod().when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());

        final String propAccessKey = "env-access-key";
        final String propSecretKey = "env-secret-key";
        doReturn(propAccessKey).when(provider).systemGetEnv(eq(ACCESS_KEY));
        doReturn(propSecretKey).when(provider).systemGetEnv(eq(SECRET_KEY));

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI_IT, null);
        assertNotNull(fileSystem);
        verifyCreationWithParams(propAccessKey, propSecretKey, S3_GLOBAL_URI_IT);
    }

    @Test
    public void createsAuthenticatedBySystemEnv() {

        doCallRealMethod().when(provider).overloadPropertiesWithSystemProps(any(Properties.class), anyString());

        final String propAccessKey = "prop-access-key";
        final String propSecretKey = "prop-secret-key";
        System.setProperty(ACCESS_KEY, propAccessKey);
        System.setProperty(SECRET_KEY, propSecretKey);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI_IT, null);
        assertNotNull(fileSystem);
        verifyCreationWithParams(propAccessKey, propSecretKey, S3_GLOBAL_URI_IT);
    }

    @Test
    public void createsAuthenticatedByUri() {
        final String accessKeyUri = "access-key-uri";
        final String secretKeyUri = "secret-key-uri";
        URI uri = URI.create("s3://" + accessKeyUri + ":" + secretKeyUri + "@s3.amazon.com");

        provider.newFileSystem(uri, null);

        verifyCreationWithParams(accessKeyUri, secretKeyUri, uri);
    }


    @Test
    public void createsAnonymousNotPossible() {
        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI_IT, ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);
        verify(provider).createFileSystem(eq(S3_GLOBAL_URI_IT), eq(buildFakeProps()));
    }

    private Map<String, String> buildFakeEnv() {
        return ImmutableMap.<String, String>builder()
                .put(ACCESS_KEY, "access-key")
                .put(SECRET_KEY, "secret-key").build();
    }

    private void verifyCreationWithParams(final String accessKeyUri, final String secretKeyUri, URI uri) {
        verify(provider).createFileSystem(eq(uri), argThat(new ArgumentMatcher<Properties>() {
            @Override
            public boolean matches(Object argument) {
                Properties called = (Properties) argument;
                assertEquals(accessKeyUri, called.get(ACCESS_KEY));
                assertEquals(secretKeyUri, called.get(SECRET_KEY));
                return true;
            }
        }));
    }

    private Properties buildFakeProps() {
        try {
            Properties props = new Properties();
            props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("amazon-test-sample.properties"));
            return props;
        } catch (IOException e) {
            throw new RuntimeException("amazon-test-sample.properties not present");
        }
    }
}