package com.upplication.s3fs.FileSystemProvider;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Owner;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.Properties;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class CheckAccessTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }
    // check access

    @Test
    public void checkAccessRead() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file");

        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir/file");
        s3fsProvider.checkAccess(file1, AccessMode.READ);
    }

    @Test(expected = AccessDeniedException.class)
    public void checkAccessReadWithoutPermission() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");

        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir");
        s3fsProvider.checkAccess(file1, AccessMode.READ);
    }

    @Test
    public void checkAccessWrite() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file");

        S3FileSystem fs = createNewS3FileSystem();
        S3Path file1 = fs.getPath("/bucketA/dir/file");
        s3fsProvider.checkAccess(file1, AccessMode.WRITE);
    }

    @Test(expected = AccessDeniedException.class)
    public void checkAccessWriteDifferentUser() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/readOnly");
        // return empty list
        doReturn(client.createReadOnly(new Owner("2", "Read Only"))).when(client).getObjectAcl("bucketA", "dir/readOnly");

        S3FileSystem fs = createNewS3FileSystem();
        S3Path file1 = fs.getPath("/bucketA/dir/readOnly");
        s3fsProvider.checkAccess(file1, AccessMode.WRITE);
    }

    @Test(expected = AccessDeniedException.class)
    public void checkAccessWriteWithoutPermission() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");
        // return empty list
        doReturn(new AccessControlList()).when(client).getObjectAcl("bucketA", "dir/");

        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir");
        s3fsProvider.checkAccess(file1, AccessMode.WRITE);
    }

    @Test(expected = AccessDeniedException.class)
    public void checkAccessExecute() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file");

        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file");

        s3fsProvider.checkAccess(file1, AccessMode.EXECUTE);
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