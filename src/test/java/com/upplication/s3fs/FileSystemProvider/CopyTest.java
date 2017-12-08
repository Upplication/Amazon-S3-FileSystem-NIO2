package com.upplication.s3fs.FileSystemProvider;

import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class CopyTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void copy() throws IOException {
        final String content = "content-file-1";
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir", "dir2").file("dir/file1", content.getBytes());

        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file1");
        Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
        s3fsProvider.copy(file, fileDest, StandardCopyOption.REPLACE_EXISTING);
        // assert
        assertTrue(Files.exists(fileDest));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
    }

    @Test
    public void copySameFile() throws IOException {
        final String content = "sample-content";
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", content.getBytes());
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA", "dir", "file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file1");
        s3fsProvider.copy(file, fileDest);
        // assert
        assertTrue(Files.exists(fileDest));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
        assertEquals(file, fileDest);
    }

    @Test
    public void copyAlreadyExistsWithReplace() throws IOException {
        final String content = "sample-content";
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", content.getBytes()).file("dir/file2");
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA", "dir", "file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file2");
        s3fsProvider.copy(file, fileDest, StandardCopyOption.REPLACE_EXISTING);
        // assert
        assertTrue(Files.exists(fileDest));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void copyAlreadyExists() throws IOException {
        final String content = "sample-content";
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", content.getBytes()).file("dir/file2", content.getBytes());
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA", "dir", "file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file2");
        s3fsProvider.copy(file, fileDest);
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