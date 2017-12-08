package com.upplication.s3fs;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.Map;

import com.amazonaws.services.s3.model.*;
import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import com.github.marschall.com.sun.nio.zipfs.ZipFileAttributeView;
import com.upplication.s3fs.S3FileStoreAttributeView.AttrID;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.UnsupportedFileStoreAttributeView;

public class S3FileStoreTest extends S3UnitTestBase {

    private S3FileSystem fileSystem;
    private S3FileStore fileStore;

    @Before
    public void prepareFileStore() throws IOException {
        Map<String, Object> env = ImmutableMap.<String, Object>builder()
                .put(ACCESS_KEY, "access-mocked")
                .put(SECRET_KEY, "secret-mocked").build();
        fileSystem = (S3FileSystem) FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);

        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucket").file("placeholder");
        client.bucket("bucket2").file("placeholder");
        S3Path path = fileSystem.getPath("/bucket/");
        fileStore = path.getFileStore();
    }

    @Test
    public void bucketConstructor() {
        S3FileStore s3FileStore = new S3FileStore(fileSystem, "name");
        assertEquals("name", s3FileStore.name());
        assertEquals("Mock", s3FileStore.getOwner().getDisplayName());
    }

    @Test
    public void nameConstructorAlreadyExists() {
        S3FileStore s3FileStore = new S3FileStore(fileSystem, "bucket2");
        assertEquals("bucket2", s3FileStore.name());
        assertEquals("Mock", s3FileStore.getOwner().getDisplayName());
    }

    @Test
    public void getFileStoreAttributeView() {
        S3FileStoreAttributeView fileStoreAttributeView = fileStore.getFileStoreAttributeView(S3FileStoreAttributeView.class);
        assertEquals("S3FileStoreAttributeView", fileStoreAttributeView.name());
        assertEquals("bucket", fileStoreAttributeView.getAttribute(AttrID.name.name()));
        assertNotNull(fileStoreAttributeView.getAttribute(AttrID.creationDate.name()));
        assertEquals("Mock", fileStoreAttributeView.getAttribute(AttrID.ownerDisplayName.name()));
        assertEquals("1", fileStoreAttributeView.getAttribute(AttrID.ownerId.name()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void getUnsupportedFileStoreAttributeView() {
        fileStore.getFileStoreAttributeView(UnsupportedFileStoreAttributeView.class);
    }

    @Test
    public void getAttributes() throws IOException {
        assertEquals("bucket", fileStore.getAttribute(AttrID.name.name()));
        assertNotNull(fileStore.getAttribute(AttrID.creationDate.name()));
        assertEquals("Mock", fileStore.getAttribute(AttrID.ownerDisplayName.name()));
        assertEquals("1", fileStore.getAttribute(AttrID.ownerId.name()));
    }

    @Test
    public void getOwner() {
        Owner owner = fileStore.getOwner();
        assertEquals("Mock", owner.getDisplayName());
        assertEquals("1", owner.getId());
    }

    @Test
    public void getRootDirectory() {
        S3Path rootDirectory = fileStore.getRootDirectory();
        assertNull("", rootDirectory.getFileName());
        assertTrue(rootDirectory.isAbsolute());
        assertEquals("s3://access-mocked@s3.test.amazonaws.com/bucket/", rootDirectory.toAbsolutePath().toString());
        assertEquals("s3://access-mocked@s3.test.amazonaws.com/bucket/", rootDirectory.toUri().toString());
    }

    @Test
    public void getSpaces() throws IOException {
        S3Path root = fileSystem.getPath("/newbucket");
        Files.createFile(root);
        S3FileStore newFileStore = root.getFileStore();
        assertEquals("newbucket", newFileStore.name());
        assertEquals("S3Bucket", newFileStore.type());
        assertEquals(false, newFileStore.isReadOnly());
        assertEquals(Long.MAX_VALUE, newFileStore.getTotalSpace());
        assertEquals(Long.MAX_VALUE, newFileStore.getUsableSpace());
        assertEquals(Long.MAX_VALUE, newFileStore.getUnallocatedSpace());
        assertFalse(newFileStore.supportsFileAttributeView(ZipFileAttributeView.class));
        assertFalse(newFileStore.supportsFileAttributeView("zip"));
    }

    @Test
    public void comparable() throws IOException {
        S3FileStore store = fileSystem.getPath("/bucket").getFileStore();
        S3FileStore other = fileSystem.getPath("/bucket1").getFileStore();
        FileSystem fileSystemLocalhost = FileSystems.newFileSystem(URI.create("s3://localhost/"), buildFakeEnv());
        FileSystem fileSystemDefault = FileSystems.getFileSystem(store.getFileSystem().getPath("/bucket").toUri());
        S3FileStore differentHost = ((S3Path) fileSystemLocalhost.getPath("/bucket")).getFileStore();
        S3FileStore shouldBeTheSame = ((S3Path) fileSystemDefault.getPath("/bucket")).getFileStore();
        S3FileStore noFs1 = new S3FileStore(null, "bucket");
        S3FileStore noFs2 = new S3FileStore(null, "bucket1");
        S3FileStore noFsSameAs1 = new S3FileStore(null, "bucket");
        S3FileStore noFsNoName1 = new S3FileStore(null, null);
        S3FileStore noFsNoName2 = new S3FileStore(null, null);
        Comparable<S3FileStore> s3FileStore = new S3FileStore(null, "bucket");

        assertEquals(0, store.compareTo(store));
        assertEquals(1, store.compareTo(other));
        assertEquals(0, store.compareTo(differentHost));
        assertEquals(0, store.compareTo(shouldBeTheSame));
        assertEquals(0, store.compareTo(noFs1));
        assertEquals(0, noFs1.compareTo(noFsSameAs1));
        assertEquals(0, s3FileStore.compareTo(noFsSameAs1));

        assertFalse(store.equals(other));
        assertFalse(store.equals(differentHost));
        assertTrue(store.equals(shouldBeTheSame));
        assertFalse(store.equals(shouldBeTheSame.getOwner()));
        assertFalse(store.equals(noFs1));
        assertFalse(noFs1.equals(store));
        assertFalse(noFs1.equals(noFs2));
        assertTrue(noFs1.equals(noFsSameAs1));
        assertFalse(noFsNoName1.equals(noFs1));
        assertTrue(noFsNoName1.equals(noFsNoName2));
    }

    private Map<String, ?> buildFakeEnv() {
        return ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access-key").put(SECRET_KEY, "secret-key").build();
    }
}