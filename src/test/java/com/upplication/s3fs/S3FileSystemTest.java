package com.upplication.s3fs;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.S3EndpointConstant;

public class S3FileSystemTest extends S3UnitTestBase {
    private FileSystem fs;

    @Before
    public void setup() throws IOException {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA");
        client.bucket("bucketB");
        fs = FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void getPathFirst() {
        assertEquals(fs.getPath("/bucket"), fs.getPath("/bucket"));
        assertEquals(fs.getPath("file"), fs.getPath("file"));
    }

    @Test
    public void getPathFirstWithMultiplesPaths() {
        assertEquals(fs.getPath("/bucket/path/to/file"), fs.getPath("/bucket/path/to/file"));
        assertNotEquals(fs.getPath("/bucket/path/other/file"), fs.getPath("/bucket/path/to/file"));

        assertEquals(fs.getPath("dir/path/to/file"), fs.getPath("dir/path/to/file"));
        assertNotEquals(fs.getPath("dir/path/other/file"), fs.getPath("dir/path/to/file"));
    }

    @Test
    public void getPathFirstAndMore() {
        Path actualAbsolute = fs.getPath("/bucket", "dir", "file");
        assertEquals(fs.getPath("/bucket", "dir", "file"), actualAbsolute);
        assertEquals(fs.getPath("/bucket/dir/file"), actualAbsolute);

        Path actualRelative = fs.getPath("dir", "dir", "file");
        assertEquals(fs.getPath("dir", "dir", "file"), actualRelative);
        assertEquals(fs.getPath("dir/dir/file"), actualRelative);
    }

    @Test
    public void getPathFirstAndMoreWithMultiplesPaths() {
        Path actual = fs.getPath("/bucket", "dir/file");
        assertEquals(fs.getPath("/bucket", "dir/file"), actual);
        assertEquals(fs.getPath("/bucket/dir/file"), actual);
        assertEquals(fs.getPath("/bucket", "dir", "file"), actual);
    }

    @Test
    public void getPathFirstWithMultiplesPathsAndMoreWithMultiplesPaths() {
        Path actual = fs.getPath("/bucket/dir", "dir/file");
        assertEquals(fs.getPath("/bucket/dir", "dir/file"), actual);
        assertEquals(fs.getPath("/bucket/dir/dir/file"), actual);
        assertEquals(fs.getPath("/bucket", "dir", "dir", "file"), actual);
        assertEquals(fs.getPath("/bucket/dir/dir", "file"), actual);
    }

    @Test
    public void getPathRelativeAndAbsoulte() {
        assertNotEquals(fs.getPath("/bucket"), fs.getPath("bucket"));
        assertNotEquals(fs.getPath("/bucket/dir"), fs.getPath("bucket/dir"));
        assertNotEquals(fs.getPath("/bucket", "dir"), fs.getPath("bucket", "dir"));
        assertNotEquals(fs.getPath("/bucket/dir", "dir"), fs.getPath("bucket/dir", "dir"));
        assertNotEquals(fs.getPath("/bucket", "dir/file"), fs.getPath("bucket", "dir/file"));
        assertNotEquals(fs.getPath("/bucket/dir", "dir/file"), fs.getPath("bucket/dir", "dir/file"));
    }

    @Test
    public void duplicatedSlashesAreDeleted() {
        Path actualFirst = fs.getPath("/bucket//file");
        assertEquals(fs.getPath("/bucket/file"), actualFirst);
        assertEquals(fs.getPath("/bucket", "file"), actualFirst);

        Path actualFirstAndMore = fs.getPath("/bucket//dir", "dir//file");
        assertEquals(fs.getPath("/bucket/dir/dir/file"), actualFirstAndMore);
        assertEquals(fs.getPath("/bucket", "dir/dir/file"), actualFirstAndMore);
        assertEquals(fs.getPath("/bucket/dir", "dir/file"), actualFirstAndMore);
        assertEquals(fs.getPath("/bucket/dir/dir", "file"), actualFirstAndMore);
    }

    @Test
    public void readOnlyAlwaysFalse() {
        assertTrue(!fs.isReadOnly());
    }

    @Test
    public void getSeparatorSlash() {
        assertEquals("/", fs.getSeparator());
        assertEquals("/", S3Path.PATH_SEPARATOR);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getPathMatcherThrowException() {
        fs.getPathMatcher("");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getUserPrincipalLookupServiceThrowException() {
        fs.getUserPrincipalLookupService();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void newWatchServiceThrowException() throws Exception {
        fs.newWatchService();
    }

    @Test(expected = IllegalArgumentException.class)
    public void getPathWithoutBucket() {
        fs.getPath("//path/to/file");
    }

    @Test
    public void getFileStores() {
        Iterable<FileStore> result = fs.getFileStores();
        assertNotNull(result);
        Iterator<FileStore> iterator = result.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
    }

    @Test
    public void getRootDirectories() {
        Iterable<Path> paths = fs.getRootDirectories();

        assertNotNull(paths);

        int size = 0;
        boolean bucketNameA = false;
        boolean bucketNameB = false;

        for (Path path : paths) {
            S3Path s3Path = (S3Path)path;
            String fileStore = s3Path.getFileStore().name();
            Path fileName = s3Path.getFileName();
            if (fileStore.equals("bucketA") && fileName == null) {
                bucketNameA = true;
            } else if (fileStore.equals("bucketB") && fileName == null) {
                bucketNameB = true;
            }
            size++;
        }

        assertEquals(2, size);
        assertTrue(bucketNameA);
        assertTrue(bucketNameB);
    }

    @Test
    public void supportedFileAttributeViewsReturnBasic() {
        Set<String> operations = fs.supportedFileAttributeViews();

        assertNotNull(operations);
        assertTrue(!operations.isEmpty());

        assertTrue(operations.contains("basic"));
        assertTrue(operations.contains("posix"));
    }

    @Test
    public void close() throws IOException {
        assertTrue(fs.isOpen());
        fs.close();
        assertTrue(!fs.isOpen());
    }

    private static void assertNotEquals(Object a, Object b) {
        assertTrue(a + " are not equal to: " + b, !a.equals(b));
    }

    @Test
    public void comparables() throws IOException {
        // crear other vars

        S3FileSystemProvider provider = new S3FileSystemProvider();
        S3FileSystem s3fs1 = (S3FileSystem) provider.newFileSystem(URI.create("s3://mirror1.amazon.test/"), buildFakeEnv());
        S3FileSystem s3fs2 = (S3FileSystem) provider.newFileSystem(URI.create("s3://mirror2.amazon.test/"), buildFakeEnv());
        S3FileSystem s3fs3 = (S3FileSystem) provider.newFileSystem(URI.create("s3://accessKey:secretKey@mirror1.amazon.test/"), null);
        S3FileSystem s3fs4 = (S3FileSystem) provider.newFileSystem(URI.create("s3://accessKey:secretKey@mirror2.amazon.test"), null);
        S3FileSystem s3fs6 = (S3FileSystem) provider.newFileSystem(URI.create("s3://access_key:secret_key@mirror1.amazon.test/"), null);
        AmazonS3ClientMock amazonClientMock = AmazonS3MockFactory.getAmazonClientMock();
        S3FileSystem s3fs7 = new S3FileSystem(provider, null, amazonClientMock, "mirror1.amazon.test", null);
        S3FileSystem s3fs8 = new S3FileSystem(provider, null, amazonClientMock, null, null);
        S3FileSystem s3fs9 = new S3FileSystem(provider, null, amazonClientMock, null, null);
        S3FileSystem s3fs10 = new S3FileSystem(provider, "somekey", amazonClientMock, null, null);
        S3FileSystem s3fs11 = new S3FileSystem(provider, "access-key@mirror2.amazon.test", amazonClientMock, "mirror2.amazon.test", null);

        // FIXME: review the hashcode creation.
        assertEquals(1483378423, s3fs1.hashCode());
        assertEquals(684416791, s3fs2.hashCode());
        assertEquals(182977201, s3fs3.hashCode());
		assertEquals(-615984431, s3fs4.hashCode());
        assertEquals(-498271993, s3fs6.hashCode());
        assertEquals(-82123487, s3fs7.hashCode());

        assertFalse(s3fs1.equals(s3fs2));
        assertFalse(s3fs1.equals(s3fs3));
        assertFalse(s3fs1.equals(s3fs4));
        assertFalse(s3fs1.equals(s3fs6));
        assertFalse(s3fs3.equals(s3fs4));
        assertFalse(s3fs3.equals(s3fs6));
        assertFalse(s3fs1.equals(s3fs6));
        assertFalse(s3fs1.equals(new S3FileStore(s3fs1, "emmer")));
        assertFalse(s3fs7.equals(s3fs8));
        assertTrue(s3fs8.equals(s3fs8));
        assertFalse(s3fs8.equals(s3fs1));
        assertTrue(s3fs8.equals(s3fs9));
        assertFalse(s3fs9.equals(s3fs10));
        assertTrue(s3fs2.equals(s3fs11));

        assertEquals(-1, s3fs1.compareTo(s3fs2));
        assertEquals(1, s3fs2.compareTo(s3fs1));
        assertEquals(-50, s3fs1.compareTo(s3fs6));

        s3fs7.close();
        s3fs8.close();
        s3fs9.close();
        s3fs10.close();
        s3fs11.close();
    }

    @Test
    public void key2Parts() {
        S3FileSystemProvider provider = new S3FileSystemProvider();
        AmazonS3ClientMock amazonClientMock = AmazonS3MockFactory.getAmazonClientMock();
        S3FileSystem s3fs = new S3FileSystem(provider, null, amazonClientMock, "mirror1.amazon.test", null);
        try {
            String[] parts = s3fs.key2Parts("/bucket/folder with spaces/file");
            assertEquals("", parts[0]);
            assertEquals("bucket", parts[1]);
            assertEquals("folder with spaces", parts[2]);
            assertEquals("file", parts[3]);
        } finally {
            try {
                s3fs.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    @Test
    public void parts2Key() {
        S3FileSystemProvider provider = new S3FileSystemProvider();
        AmazonS3ClientMock amazonClientMock = AmazonS3MockFactory.getAmazonClientMock();
        S3FileSystem s3fs = new S3FileSystem(provider, null, amazonClientMock, "mirror1.amazon.test", null);
        S3Path path = s3fs.getPath("/bucket", "folder with spaces", "file");
        try {
            assertEquals("folder with spaces/file", path.getKey());
        } finally {
            try {
                s3fs.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    @Test
    public void urlWithSpecialCharacters() throws IOException {
        String fileName = "βeta.png";
        String expected = "https://bucket.s3.eu-west-1.amazonaws.com/%CE%B2eta.png";

        AmazonS3 amazonS3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .build();
        S3FileSystem s3FileSystem = new S3FileSystem(null, null, amazonS3Client, "mirror", null);
        S3Path path = new S3Path(s3FileSystem, fileName);

        String url = amazonS3Client.getUrl("bucket", path.getKey()).toString();

        assertEquals(expected, url);
    }

    @Test
    public void urlWithSpaceCharacters() throws IOException {
        String fileName = "beta gaming.png";
        String expected = "https://bucket.s3.eu-west-1.amazonaws.com/beta%20gaming.png";

        AmazonS3 amazonS3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .build();
        S3FileSystem s3FileSystem = new S3FileSystem(null, null, amazonS3Client, "mirror", null);
        S3Path path = new S3Path(s3FileSystem, fileName);

        String url = amazonS3Client.getUrl("bucket", path.getKey()).toString();

        assertEquals(expected, url);
    }

    @Test
    public void createDirectory() throws IOException {
        S3FileSystemProvider provider = new S3FileSystemProvider();
        AmazonS3ClientMock amazonClientMock = AmazonS3MockFactory.getAmazonClientMock();
        S3FileSystem s3fs = new S3FileSystem(provider, null, amazonClientMock, "mirror1.amazon.test", null);
        try {
            S3Path folder = s3fs.getPath("/bucket", "folder");
            provider.createDirectory(folder);
            assertTrue(Files.exists(folder));
        } finally {
            try {
                s3fs.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createDirectoryWithAttributes() throws IOException {
        S3FileSystemProvider provider = new S3FileSystemProvider();
        AmazonS3ClientMock amazonClientMock = AmazonS3MockFactory.getAmazonClientMock();
        S3FileSystem s3fs = new S3FileSystem(provider, null, amazonClientMock, "mirror1.amazon.test", null);
        try {
            S3Path folder = s3fs.getPath("/bucket", "folder");
            provider.createDirectory(folder, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrw")));
        } finally {
            try {
                s3fs.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    @Test
    public void isSameFile() throws IOException {
        S3FileSystemProvider provider = new S3FileSystemProvider();
        AmazonS3ClientMock amazonClientMock = AmazonS3MockFactory.getAmazonClientMock();
        S3FileSystem s3fs = new S3FileSystem(provider, null, amazonClientMock, "mirror1.amazon.test", null);
        try {
            S3Path folder = s3fs.getPath("/bucket", "folder");
            S3Path sameFolder = s3fs.getPath("/bucket", "folder");
            S3Path differentFolder = s3fs.getPath("/bucket", "folder2");
            Path relativize = folder.getParent().relativize(folder);
            assertTrue(provider.isSameFile(folder, sameFolder));
            assertFalse(provider.isSameFile(folder, differentFolder));
            assertFalse(provider.isSameFile(folder, relativize));
            assertFalse(provider.isSameFile(relativize, folder));
        } finally {
            try {
                s3fs.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private Map<String, ?> buildFakeEnv() {
        return ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access-key").put(SECRET_KEY, "secret-key").build();
    }
}