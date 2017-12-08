package com.upplication.s3fs.FileSystemProvider;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.IOUtils;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.*;
import java.util.Map;
import java.util.Properties;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class NewInputStreamTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }


    @Test
    public void inputStreamFile() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").file("file1", "content".getBytes());

        Path file = createNewS3FileSystem().getPath("/bucketA/file1");
        try (InputStream inputStream = s3fsProvider.newInputStream(file)) {

            byte[] buffer = IOUtils.toByteArray(inputStream);
            // check
            assertArrayEquals("content".getBytes(), buffer);
        }
    }

    @Test
    public void anotherInputStreamFile() throws IOException {
        String res = "another content";
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", res.getBytes());
        // act
        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file1");

        try (InputStream inputStream = s3fsProvider.newInputStream(file)) {

            byte[] buffer = IOUtils.toByteArray(inputStream);
            // check
            assertArrayEquals(res.getBytes(), buffer);
        }
    }

    @Test(expected = NoSuchFileException.class)
    public void newInputStreamFileNotExists() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");
        // act
        S3FileSystem fileSystem = createNewS3FileSystem();
        Path file = fileSystem.getPath("/bucketA/dir/file1");
        try (InputStream inputStream = s3fsProvider.newInputStream(file)) {
            fail("file not exists");
        }
    }

    @Test(expected = IOException.class)
    public void inputStreamDirectory() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");
        Path result = s3fsProvider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
        // act
        s3fsProvider.newInputStream(result);
    }

    private Map<String, ?> buildFakeEnv() {
        return ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "accesskey").put(SECRET_KEY, "secretkey").build();
    }

    /**
     * create a new file system for s3 scheme with fake credentials
     * and global endpoint
     *
     * @return FileSystem
     * @throws IOException
     */
    private S3FileSystem createNewS3FileSystem() throws IOException {
        try {
            return s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        } catch (FileSystemNotFoundException e) {
            return (S3FileSystem) FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        }

    }
}