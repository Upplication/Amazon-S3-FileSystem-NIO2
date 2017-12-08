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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class MoveTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }


    @Test
    public void move() throws IOException {
        // fixtures
        final String content = "sample-content";
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir", "dir2").file("dir/file1", content.getBytes());
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file1");
        Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
        s3fsProvider.move(file, fileDest);
        // assert
        assertTrue(Files.exists(fileDest));
        assertTrue(Files.notExists(file));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
    }

    @Test
    public void moveWithReplaceExisting() throws IOException {
        // fixtures
        final String content = "sample-content";
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir")
                .file("dir/file1", content.getBytes())
                .file("dir/file2", "different-content".getBytes());
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file2");
        s3fsProvider.move(file, fileDest, StandardCopyOption.REPLACE_EXISTING);
        // assert
        assertTrue(Files.exists(fileDest));
        assertTrue(Files.notExists(file));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void moveWithoutReplaceExisting() throws IOException {
        // fixtures
        final String content = "sample-content";
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir")
                .file("dir/file1", content.getBytes())
                .file("dir/file2", "different-content".getBytes());
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file2");
        s3fsProvider.move(file, fileDest);
    }

    @Test(expected = AtomicMoveNotSupportedException.class)
    public void moveWithAtomicOption() throws IOException {
        // fixtures
        final String content = "sample-content";
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir", "dir2").file("dir/file1", content.getBytes());
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file1");
        Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
        s3fsProvider.move(file, fileDest, StandardCopyOption.ATOMIC_MOVE);
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