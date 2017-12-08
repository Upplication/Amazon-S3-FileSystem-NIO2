package com.upplication.s3fs.FileSystemProvider;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.Properties;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class DeleteTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void deleteFile() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file");
        // act
        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        s3fsProvider.delete(file);
        // assert
        assertTrue(Files.notExists(file));
    }

    @Test
    public void deleteEmptyDirectory() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");
        Path base = s3fsProvider.newFileSystem(URI.create("s3://endpoint1/"),
                ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access_key").put(SECRET_KEY, "secret_key").build())
                .getPath("/bucketA/dir");
        // act
        s3fsProvider.delete(base);
        // assert
        assertTrue(Files.notExists(base));
    }

    @Test(expected = DirectoryNotEmptyException.class)
    public void deleteDirectoryWithEntries() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file");

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        s3fsProvider.delete(file.getParent());
    }

    @Test(expected = NoSuchFileException.class)
    public void deleteFileNotExists() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        s3fsProvider.delete(file);
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