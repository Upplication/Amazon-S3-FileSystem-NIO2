package com.upplication.s3fs.Path;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Map;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.util.S3EndpointConstant.*;

import static org.junit.Assert.assertEquals;

public class ToUriTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void toUri() {
        Path path = getPath("/bucket/path/to/file");
        URI uri = path.toUri();

        // the scheme is s3
        assertEquals("s3", uri.getScheme());

        // could get the correct fileSystem
        S3FileSystem fs =  s3fsProvider.getFileSystem(uri);
        // the host is the endpoint specified in fileSystem
        assertEquals(fs.getEndpoint(), uri.getHost());

        // bucket name as first path
        Path pathActual = fs.provider().getPath(uri);

        assertEquals(path, pathActual);
    }

    @Test
    public void toUriWithEndSlash() {
        S3Path s3Path = getPath("/bucket/folder/");

        assertEquals(S3_GLOBAL_URI_TEST + "bucket/folder/", s3Path.toUri().toString());
    }

    @Test
    public void toUriWithSpaces() {
        S3Path s3Path = getPath("/bucket/with spaces");

        assertEquals(S3_GLOBAL_URI_TEST.resolve("bucket/with%20spaces") , s3Path.toUri());
    }

    @Test
    public void toUriWithNotEndSlash() {
        S3Path s3Path = getPath("/bucket/file");

        assertEquals(S3_GLOBAL_URI_TEST + "bucket/file", s3Path.toUri().toString());
    }

    @Test
    public void toUriRelative() {
        S3FileSystem fileSystem = s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST);

        S3Path path = new S3Path(fileSystem, "bla");
        assertEquals(URI.create("bla"), path.toUri());
        
        S3Path withSpaces = new S3Path(fileSystem, "with space");
        assertEquals(URI.create("with%20space"), withSpaces.toUri());
    }

    @Test
    public void toUriRelativeWithSpaces() {
        S3FileSystem fileSystem = s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST);

        S3Path path = new S3Path(fileSystem, "with space");
        assertEquals(URI.create("with%20space"), path.toUri());
    }

    @Test
    public void toUriBucketWithoutEndSlash() {
        S3Path s3Path = getPath("/bucket");

        assertEquals(S3_GLOBAL_URI_TEST.resolve("/bucket/"), s3Path.toUri());
    }

    @Test
    public void toUriWithCredentials() {
        Map<String, String> envs = ImmutableMap.<String, String>builder().put(ACCESS_KEY, "access").put(SECRET_KEY, "secret").build();
        FileSystem fileSystem = s3fsProvider.newFileSystem(S3_GLOBAL_URI_TEST, envs);

        Path path = fileSystem.getPath("/bla/file");

        assertEquals(URI.create("s3://access@s3.test.amazonaws.com/bla/file"), path.toUri());
    }

    @Test
    public void toUriWithCredentialBySystemProperty() {

        System.setProperty(ACCESS_KEY, "accessKeywii");
        System.setProperty(SECRET_KEY, "secretKey");

        FileSystem fileSystem = s3fsProvider.newFileSystem(S3_GLOBAL_URI_TEST, null);

        Path path = fileSystem.getPath("/bla/file");

        assertEquals(URI.create("s3://accessKeywii@s3.test.amazonaws.com/bla/file"), path.toUri());

        System.clearProperty(ACCESS_KEY);
        System.clearProperty(SECRET_KEY);
    }

    @Test
    public void toUriWithEndpoint() throws IOException {
        try (FileSystem fs = s3fsProvider.newFileSystem(URI.create("s3://endpoint/"), null)) {
            Path path = fs.getPath("/bucket/path/to/file");
            URI uri = path.toUri();
            // the scheme is s3
            assertEquals("s3", uri.getScheme());
            assertEquals("endpoint", uri.getHost());
            assertEquals("/bucket/path/to/file", uri.getPath());
        }
    }

    private S3Path getPath(String path) {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }
}
