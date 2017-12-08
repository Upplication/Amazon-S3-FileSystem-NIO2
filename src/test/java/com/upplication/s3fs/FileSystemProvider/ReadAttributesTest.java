package com.upplication.s3fs.FileSystemProvider;

import com.google.common.collect.Sets;
import com.upplication.s3fs.*;
import com.upplication.s3fs.util.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.upplication.s3fs.util.FileAttributeBuilder.build;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ReadAttributesTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }


    // readAttributes BasicFileAttributes.class

    @Test
    public void readAttributesFileEmpty() throws IOException {
        // fixtures
        final String content = "";
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", content.getBytes());

        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");

        BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(file1, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.isDirectory());
        assertEquals(true, fileAttributes.isRegularFile());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(0L, fileAttributes.size());
    }

    @Test
    public void readAttributesFile() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        MockBucket mocket = client.bucket("bucketA").dir("dir");

        final String content = "sample";
        Path memoryFile = Files.write(mocket.resolve("dir/file"), content.getBytes());

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryFile, BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(file, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.isDirectory());
        assertEquals(true, fileAttributes.isRegularFile());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(content.getBytes().length, fileAttributes.size());
        assertEquals("dir/file", fileAttributes.fileKey());
        assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.lastModifiedTime());
        // TODO: creation and access are the same that last modified time
        assertEquals(fileAttributes.lastModifiedTime(), fileAttributes.creationTime());
        assertEquals(fileAttributes.lastModifiedTime(), fileAttributes.lastAccessTime());
    }

    @Test
    public void readAttributesDirectory() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        MockBucket mocket = client.bucket("bucketA").dir("dir");
        Path memoryDir = mocket.resolve("dir/");

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryDir, BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path dir = fs.getPath("/bucketA/dir");

        BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(dir, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertEquals(true, fileAttributes.isDirectory());
        assertEquals(false, fileAttributes.isRegularFile());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(0L, fileAttributes.size());
        assertEquals("dir/", fileAttributes.fileKey());
        assertEquals(expectedAttributes.lastModifiedTime().to(TimeUnit.SECONDS), fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
        assertEquals(expectedAttributes.creationTime().to(TimeUnit.SECONDS), fileAttributes.creationTime().to(TimeUnit.SECONDS));
        assertEquals(expectedAttributes.lastAccessTime().to(TimeUnit.SECONDS), fileAttributes.lastAccessTime().to(TimeUnit.SECONDS));
        // TODO: creation and access are the same that last modified time
        assertEquals(fileAttributes.creationTime().to(TimeUnit.SECONDS), fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
        assertEquals(fileAttributes.lastAccessTime().to(TimeUnit.SECONDS), fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
    }

    @Test
    public void readAnotherAttributesDirectory() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/dir", "content".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path dir = fs.getPath("/bucketA/dir");

        BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(dir, BasicFileAttributes.class);
        assertNotNull(fileAttributes);
        assertEquals(true, fileAttributes.isDirectory());
    }

    @Test
    public void readAttributesDirectoryNotExistsAtAmazon() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        MockBucket mocket = client.bucket("bucketA").dir("dir", "dir/dir2");
        Path memoryDir = mocket.resolve("dir/dir2/");

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryDir, BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path dir = fs.getPath("/bucketA/dir");

        BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(dir, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertEquals(true, fileAttributes.isDirectory());
        assertEquals(false, fileAttributes.isRegularFile());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(0L, fileAttributes.size());
        assertEquals("dir/", fileAttributes.fileKey());
        assertEquals(expectedAttributes.lastModifiedTime().to(TimeUnit.SECONDS), fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
        assertEquals(expectedAttributes.creationTime().to(TimeUnit.SECONDS), fileAttributes.creationTime().to(TimeUnit.SECONDS));
        assertEquals(expectedAttributes.lastAccessTime().to(TimeUnit.SECONDS), fileAttributes.lastAccessTime().to(TimeUnit.SECONDS));
        // TODO: creation and access are the same that last modified time
        assertEquals(fileAttributes.creationTime().to(TimeUnit.SECONDS), fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
        assertEquals(fileAttributes.lastAccessTime().to(TimeUnit.SECONDS), fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
    }

    @Test
    public void readAttributesRegenerateCacheWhenNotExists() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", "".getBytes());

        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");
        // create the cache
        s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
        assertNotNull(((S3Path) file1).getFileAttributes());

        s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
        assertNull(((S3Path) file1).getFileAttributes());

        s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
        assertNotNull(((S3Path) file1).getFileAttributes());
    }

    @Test
    public void readAttributesPosixRegenerateCacheWhenNotExists() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", "".getBytes());

        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");
        // create the cache
        s3fsProvider.readAttributes(file1, PosixFileAttributes.class);
        assertNotNull(((S3Path) file1).getFileAttributes());

        s3fsProvider.readAttributes(file1, PosixFileAttributes.class);
        assertNull(((S3Path) file1).getFileAttributes());

        s3fsProvider.readAttributes(file1, PosixFileAttributes.class);
        assertNotNull(((S3Path) file1).getFileAttributes());
    }

    @Test(expected = NoSuchFileException.class)
    public void readAttributesFileNotExists() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");

        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir/file1");

        s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
    }

    @Test(expected = NoSuchFileException.class)
    public void readAttributesFileNotExistsButExistsAnotherThatContainsTheKey() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1hola", "content".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir/file1");

        s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readAttributesNotAcceptedSubclass() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");

        FileSystem fs = createNewS3FileSystem();
        Path dir = fs.getPath("/bucketA/dir");
        s3fsProvider.readAttributes(dir, DosFileAttributes.class);
    }

    // readAttributes permission PosixFileAttributes.class

    @Test
    public void readPosixPermissionOwnerWriteAttributes() throws IOException {

        // fixtures
        AmazonS3MockFactory.getAmazonClientMock()
                .bucket("bucketA").file("file", "content".getBytes(),
                build("posix:permissions", Sets.newHashSet(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_WRITE));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
    }

    @Test
    public void readPosixPermissionOwnerExecuteAttributes() throws IOException {

        // fixtures
        AmazonS3MockFactory.getAmazonClientMock()
                .bucket("bucketA").file("file", "content".getBytes(),
                build("posix:permissions", Sets.newHashSet(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_EXECUTE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_EXECUTE));
    }

    @Test
    public void readPosixPermissionGroupWriteAttributes() throws IOException {

        // fixtures
        AmazonS3MockFactory.getAmazonClientMock()
                .bucket("bucketA").file("file", "content".getBytes(),
                build("posix:permissions", Sets.newHashSet(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.GROUP_WRITE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_WRITE));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
    }

    @Test
    public void readPosixPermissionGroupExecuteAttributes() throws IOException {

        // fixtures
        AmazonS3MockFactory.getAmazonClientMock()
                .bucket("bucketA").file("file", "content".getBytes(),
                build("posix:permissions", Sets.newHashSet(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.GROUP_EXECUTE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_EXECUTE));
    }

    @Test
    public void readPosixPermissionOtherWriteAttributes() throws IOException {

        // fixtures
        AmazonS3MockFactory.getAmazonClientMock()
                .bucket("bucketA").file("file", "content".getBytes(),
                build("posix:permissions", Sets.newHashSet(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OTHERS_WRITE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_WRITE));
    }

    @Test
    public void readPosixPermissionOtherExecuteAttributes() throws IOException {

        // fixtures
        AmazonS3MockFactory.getAmazonClientMock()
                .bucket("bucketA").file("file", "content".getBytes(),
                build("posix:permissions", Sets.newHashSet(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OTHERS_EXECUTE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_EXECUTE));
    }

    // readAttributes owner and group PosixFileAttributes

    @Test
    public void readPosixPermissionOwnerAndGroupAttributes() throws IOException {

        // fixtures
        AmazonS3MockFactory.getAmazonClientMock()
                .bucket("bucketA").file("file", "content".getBytes(),
                build("posix:permissions", Sets.newHashSet(PosixFilePermission.OWNER_READ)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertNotNull(fileAttributes.owner());
        assertNull(fileAttributes.group());
    }

    // readAttributes String all

    @Test
    public void readAttributesAll() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        final String content = "sample";
        Path memoryFile = Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), content.getBytes());

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryFile, BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "*");

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.get("isDirectory"));
        assertEquals(true, fileAttributes.get("isRegularFile"));
        assertEquals(false, fileAttributes.get("isSymbolicLink"));
        assertEquals(false, fileAttributes.get("isOther"));
        assertEquals((long) content.getBytes().length, fileAttributes.get("size"));
        assertEquals("dir/file", fileAttributes.get("fileKey"));
        assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.get("lastModifiedTime"));
        assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.get("creationTime"));
        assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.get("lastAccessTime"));
    }

    @Test
    public void readAttributesAllBasic() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "basic:*");
        Map<String, Object> fileAttributes2 = s3fsProvider.readAttributes(file, "*");

        assertArrayEquals(fileAttributes.values().toArray(new Object[]{}),
                fileAttributes2.values().toArray(new Object[]{}));
        assertArrayEquals(fileAttributes.keySet().toArray(new String[]{}),
                fileAttributes2.keySet().toArray(new String[]{}));
    }

    @Test
    public void readAttributesOnlyOne() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "isDirectory");

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.get("isDirectory"));
        assertEquals(1, fileAttributes.size());
    }

    @Test
    public void readAttributesPartial() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "isDirectory,isRegularFile");

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.get("isDirectory"));
        assertEquals(true, fileAttributes.get("isRegularFile"));
        assertEquals(2, fileAttributes.size());
    }

    @Test
    public void readAttributesPartialBasic() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "basic:isOther,basic:creationTime");
        Map<String, Object> fileAttributes2 = s3fsProvider.readAttributes(file, "isOther,creationTime");

        assertArrayEquals(fileAttributes.values().toArray(new Object[]{}),
                fileAttributes2.values().toArray(new Object[]{}));
        assertArrayEquals(fileAttributes.keySet().toArray(new String[]{}),
                fileAttributes2.keySet().toArray(new String[]{}));
    }

    @Test
    public void readAttributesAllPosix() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "posix:*");

        assertNotNull(fileAttributes.get("permissions"));
    }

    @Test
    public void readAttributesPartialPosix() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "posix:permissions");

        assertNotNull(fileAttributes.get("permissions"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void readAttributesNullAttrs() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        s3fsProvider.readAttributes(file, (String) null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readAttributesDosNotSupported() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        s3fsProvider.readAttributes(file, "dos:*");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readAttributesUnknowNotSupported() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        s3fsProvider.readAttributes(file, "lelel:*");
    }

    // setAttribute

    @Test(expected = UnsupportedOperationException.class)
    public void readAttributesObject() throws IOException {
        s3fsProvider.setAttribute(null, "", new Object());
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