package com.upplication.s3fs.Path;

import com.upplication.s3fs.AmazonS3Factory;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.util.HashMap;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.*;

public class S3PathTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    private S3Path forPath(String path) {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void createNoPath() {
        S3Path path = forPath("/bucket");

        assertEquals("bucket", path.getFileStore().name());
        assertEquals("", path.getKey());
    }

    @Test
    public void createWithTrailingSlash() {
        S3Path path = forPath("/bucket/");

        assertEquals(path.getFileStore().name(), "bucket");
        assertEquals(path.getKey(), "");
    }

    @Test
    public void createWithPath() {
        S3Path path = forPath("/bucket/path/to/file");

        assertEquals(path.getFileStore().name(), "bucket");
        assertEquals(path.getKey(), "path/to/file");
    }

    @Test
    public void createWithPathAndTrailingSlash() {
        S3Path path = forPath("/bucket/path/to/dir/");

        assertEquals("bucket", path.getFileStore().name());
        assertEquals("path/to/dir/", path.getKey());

    }

    @Test
    public void createWithPathAndTrailingSlashDir() {
        S3Path path = forPath("/bucket/path/to/dir/");

        assertEquals("bucket", path.getFileStore().name());
        assertEquals("path/to/dir/", path.getKey());
    }

    @Test
    public void createRelative() {
        S3Path path = forPath("path/to/file");

        assertNull(path.getFileStore());
        assertEquals(path.getKey(), "path/to/file");
        assertFalse(path.isAbsolute());
    }

    @Test
    public void getParent() {
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/file").getParent());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/file/").getParent());
        assertNull(forPath("/bucket/").getParent());
        assertNull(forPath("/bucket").getParent());
    }

    @Test
    public void nameCount() {
        assertEquals(forPath("/bucket/path/to/file").getNameCount(), 3);
        assertEquals(forPath("/bucket/").getNameCount(), 0);
    }


    @Test
    public void relativize() {
        Path path = forPath("/bucket/path/to/file");
        Path other = forPath("/bucket/path/to/file/hello");

        assertEquals(forPath("hello"), path.relativize(other));

        // another

        assertEquals(forPath("file/hello"), forPath("/bucket/path/to/").relativize(forPath("/bucket/path/to/file/hello")));

        // empty

        assertEquals(forPath(""), forPath("/bucket/path/to/").relativize(forPath("/bucket/path/to/")));
    }


    // register

    @Test(expected = UnsupportedOperationException.class)
    public void registerWithEventsThrowException() throws IOException {
        forPath("file1").register(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void registerThrowException() throws IOException {
        forPath("file1").register(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void registerWithEventsAndModiferThrowException() throws IOException {
        forPath("file1").register(null);
    }

    // to file

    @Test(expected = UnsupportedOperationException.class)
    public void toFile() {
        forPath("file1").toFile();
    }

    // compares to

    @Test
    public void compare() {
        assertTrue(forPath("file1").compareTo(forPath("file1")) == 0);
        assertTrue(forPath("/path/file1").compareTo(forPath("/path/file1")) == 0);
        assertTrue(forPath("/A/file1").compareTo(forPath("/B/file1")) == -1);
        assertTrue(forPath("/B/file1").compareTo(forPath("/A/file1")) == 1);
        assertTrue(forPath("/AA/file1").compareTo(forPath("/A/file1")) > 0);
        assertTrue(forPath("a").compareTo(forPath("aa")) < 0);
        assertTrue(forPath("ab").compareTo(forPath("aa")) > 0);
    }

    // toRealPath

    @Test
    public void toRealPathThrowException() throws IOException {
        Path path = forPath("/file1");
        Path other = path.toRealPath();
        assertEquals(path, other);
    }

    // toAbsolutePath

    @SuppressWarnings("unused")
    @Test(expected = IllegalStateException.class)
    public void toAbsolutePathRelativePathThrowException() throws IOException {
        forPath("file1").toAbsolutePath();
    }

    @Test
    public void toAbsolutePath() {
        Path path = forPath("/file1");
        Path other = path.toAbsolutePath();
        assertEquals(path, other);
    }

    @Test
    public void hashCodeHashMap() {
        HashMap<S3Path, String> hashMap = new HashMap<>();
        hashMap.put(forPath("/bucket/a"), "a");
        hashMap.put(forPath("/bucket/a"), "b");

        assertEquals(1, hashMap.size());
        assertEquals("b", hashMap.get(forPath("/bucket/a")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void preconditions() {
        S3FileSystem fileSystem = s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        new S3Path(fileSystem, "/");
    }

    @Test
    public void constructors() {
        S3FileSystem fileSystem = s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        S3Path path = new S3Path(fileSystem, "/buckname");
        assertEquals("buckname", path.getFileStore().name());
        assertEquals("", path.getKey());
        assertNull(path.getParent());
        assertEquals("", path.getKey());
        path = new S3Path(fileSystem, "/buckname/");
        assertEquals("buckname", path.getFileStore().name());
        assertEquals("", path.getKey());
        path = new S3Path(fileSystem, "/buckname/file");
        assertEquals("buckname", path.getFileStore().name());
        assertEquals("file", path.getKey());
        path = new S3Path(fileSystem, "/buckname/dir/file");
        assertEquals("buckname", path.getFileStore().name());
        assertEquals("dir/file", path.getKey());
        path = new S3Path(fileSystem, "dir/file");
        assertNull(path.getFileStore());
        assertEquals("dir/file", path.getKey());
        assertEquals("dir/", path.getParent().toString());
        path = new S3Path(fileSystem, "bla");
        assertNull(path.getFileStore());
        assertEquals("bla", path.getKey());
        assertNull(path.getParent());
        path = new S3Path(fileSystem, "");
        assertNull(path.getFileStore());
        assertEquals("", path.getKey());
        assertNull(path.getParent());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void register() throws IOException {
        S3Path path = forPath("/buck/file");
        path.register(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void registerWatchService() throws IOException {
        S3Path path = forPath("/buck/file");
        path.register(null, new WatchEvent.Kind<?>[0], new WatchEvent.Modifier[0]);
    }
	@Test
	public void testResolve() throws IOException {
		S3FileSystem fileSystem2 = null;
		try {
			HashMap<String, String> environments = new HashMap<>();
			environments.put(AmazonS3Factory.ACCESS_KEY, "accesskey");
			environments.put(AmazonS3Factory.SECRET_KEY, "secretaccesskey");

			fileSystem2 = (S3FileSystem) FileSystems.newFileSystem(URI.create("s3://accesskey:secretaccesskey@s3.test.amazonaws.com/bucket"), environments);

			//good
			S3Path parent = (S3Path) fileSystem2.provider().getPath(URI.create("s3://accesskey:secretaccesskey@s3.test.amazonaws.com/bucket"));
			S3Path child = (S3Path) fileSystem2.provider().getPath(URI.create("s3://accesskey:secretaccesskey@s3.test.amazonaws.com/bucket/rabbit"));
			S3Path resolved = (S3Path) parent.resolve(child);

			assertEquals(child, resolved);

			resolved = (S3Path) parent.resolve("s3://accesskey:secretaccesskey@s3.test.amazonaws.com/bucket/rabbit");
			assertEquals(child, resolved);

			resolved = (S3Path) parent.resolve("rabbit");
			assertEquals(child, resolved);

			resolved = (S3Path) parent.resolve(Paths.get("rabbit")); //unixPath
			assertEquals(child, resolved);

			resolved = (S3Path) parent.resolve(Paths.get("./rabbit")); //unixPath
			assertEquals("s3://accesskey:secretaccesskey@s3.test.amazonaws.com/bucket/./rabbit", resolved.toString());

			resolved = (S3Path) parent.resolve(Paths.get("./rabbit in space")); //unixPath
			assertEquals("s3://accesskey:secretaccesskey@s3.test.amazonaws.com/bucket/./rabbit%20in%20space", resolved.toString());

			try {
				parent.resolve(Paths.get("/tmp"));
				fail("expect IllegalArgumentException");
			} catch (IllegalArgumentException e) {
				//ignore
				assertEquals("other must be an instance of com.upplication.s3fs.S3Path or be relative", e.getMessage());
			}

			//bad
			S3Path parent2 = fileSystem2.getPath("s3://accesskey:secretaccesskey@s3.test.amazonaws.com/bucket");
			S3Path child2 = fileSystem2.getPath("s3://accesskey:secretaccesskey@s3.test.amazonaws.com/bucket/rabbit");
			S3Path resolved2 = (S3Path) parent2.resolve(child2);
			assertEquals("s3:/accesskey:secretaccesskey@s3.test.amazonaws.com/bucket/s3:/accesskey:secretaccesskey@s3.test.amazonaws.com/bucket/rabbit", resolved2.toString());

		} finally {
			if (fileSystem2 != null)
				fileSystem2.close();
		}
	}
}