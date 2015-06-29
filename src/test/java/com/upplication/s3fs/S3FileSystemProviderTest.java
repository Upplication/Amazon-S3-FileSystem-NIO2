package com.upplication.s3fs;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS;
import static com.upplication.s3fs.S3FileSystemProvider.CHARSET_KEY;
import static org.junit.Assert.*;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.upplication.s3fs.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Owner;
import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.MockBucket;
import org.mockito.ArgumentMatcher;

public class S3FileSystemProviderTest extends S3UnitTestBase {

	private S3FileSystemProvider s3fsProvider;

	@Before
	public void setup() {
		s3fsProvider = spy(new S3FileSystemProvider());
        doReturn(false).when(s3fsProvider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
		doReturn(new Properties()).when(s3fsProvider).loadAmazonProperties();
	}

	@Test(expected = S3FileSystemConfigurationException.class)
	public void missconfigure() {
		Properties props = new Properties();
		props.setProperty(AMAZON_S3_FACTORY_CLASS, "com.upplication.s3fs.util.BrokenAmazonS3Factory");
		s3fsProvider.createFileSystem(S3_GLOBAL_URI, props);
	}

	@Test
	public void createsAuthenticatedByEnv() {
		Map<String, ?> env = buildFakeEnv();
		FileSystem fileSystem = s3fsProvider.newFileSystem(S3_GLOBAL_URI, env);
		assertNotNull(fileSystem);
		verify(s3fsProvider).createFileSystem(eq(S3_GLOBAL_URI), eq(buildFakeProps((String) env.get(ACCESS_KEY), (String) env.get(SECRET_KEY))));
	}

	@Test
	public void setEncodingByProperties() {
		Properties props = new Properties();
		props.setProperty(SECRET_KEY, "better secret key");
		props.setProperty(ACCESS_KEY, "better access key");
		props.setProperty(CHARSET_KEY, "UTF-8");
		doReturn(props).when(s3fsProvider).loadAmazonProperties();
		URI uri = S3_GLOBAL_URI;

		FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);

		verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps("better access key", "better secret key", "UTF-8")));
	}

	@Test
	public void createAuthenticatedByProperties() {
		Properties props = new Properties();
		props.setProperty(SECRET_KEY, "better secret key");
		props.setProperty(ACCESS_KEY, "better access key");
		doReturn(props).when(s3fsProvider).loadAmazonProperties();
		URI uri = S3_GLOBAL_URI;

		FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);

		verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps("better access key", "better secret key")));
	}

	@Test
	public void createAuthenticatedBySystemEnvironment() {

        final String accessKey = "better-access-key";
        final String secretKey = "better-secret-key";

        doReturn(accessKey).when(s3fsProvider).systemGetEnv(ACCESS_KEY);
        doReturn(secretKey).when(s3fsProvider).systemGetEnv(SECRET_KEY);
        doCallRealMethod().when(s3fsProvider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());

		s3fsProvider.newFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());

		verify(s3fsProvider).createFileSystem(eq(S3_GLOBAL_URI), argThat(new ArgumentMatcher<Properties>() {
            @Override
            public boolean matches(Object argument) {
                Properties called = (Properties)argument;
                assertEquals(accessKey, called.getProperty(ACCESS_KEY));
                assertEquals(secretKey, called.getProperty(SECRET_KEY));
                return true;
            }
        }));
	}

	@Test
	public void createsAnonymous() {
		URI uri = S3_GLOBAL_URI;
		FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps(null, null)));
	}

    @Test
    public void createWithDefaultEndpoint() {
        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "better secret key");
        props.setProperty(ACCESS_KEY, "better access key");
        props.setProperty(CHARSET_KEY, "UTF-8");
        doReturn(props).when(s3fsProvider).loadAmazonProperties();
        URI uri = URI.create("s3:///");

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object> of());
        assertNotNull(fileSystem);

        verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps("better access key", "better secret key", "UTF-8")));
    }

	@Test(expected = IllegalArgumentException.class)
	public void createWithOnlyAccessKey() {
		Properties props = new Properties();
		props.setProperty(ACCESS_KEY, "better access key");
		doReturn(props).when(s3fsProvider).loadAmazonProperties();
		s3fsProvider.newFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
	}

	@Test(expected = IllegalArgumentException.class)
	public void createWithOnlySecretKey() {
		Properties props = new Properties();
		props.setProperty(SECRET_KEY, "better secret key");
		doReturn(props).when(s3fsProvider).loadAmazonProperties();
		s3fsProvider.newFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void createFailsIfAlreadyCreated() {
		FileSystem fileSystem = s3fsProvider.newFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		s3fsProvider.newFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
	}

	@Test(expected = IllegalArgumentException.class)
	public void createWithWrongEnv() {
		Map<String, Object> env = ImmutableMap.<String, Object> builder().put(ACCESS_KEY, 1234).put(SECRET_KEY, "secret key").build();
		FileSystem fileSystem = s3fsProvider.newFileSystem(S3_GLOBAL_URI, env);
		assertNotNull(fileSystem);
		s3fsProvider.newFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
	}

	@Test
	public void getFileSystem() {
		FileSystem fileSystem = s3fsProvider.newFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		fileSystem = s3fsProvider.getFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		FileSystem other = s3fsProvider.getFileSystem(S3_GLOBAL_URI);
		assertSame(fileSystem, other);
	}

	@Test
	public void getUnknownFileSystem() {
		FileSystem fileSystem = s3fsProvider.getFileSystem(URI.create("s3://endpoint20/bucket/path/to/file"), ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
	}

	@Test
	public void getPathWithEmtpyEndpoint() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3:///bucket/path/to/file"));

		assertEquals(path, fs.getPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}

	@Test
	public void getPath() throws IOException {

		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), null);
		Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

		assertEquals(path, fs.getPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}

	@Test
	public void getAnotherPath() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

		assertEquals(path, fs.getPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}

	@Test(expected = IllegalArgumentException.class)
	public void getPathWithEndpointAndWithoutBucket() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), null);
		fs.provider().getPath(URI.create("s3://endpoint1//falta-bucket"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void getPathWithDefaultEndpointAndWithoutBucket() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object> of());
		fs.provider().getPath(URI.create("s3:////falta-bucket"));
	}

	@Test
	public void closeFileSystemReturnNewFileSystem() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		Map<String, ?> env = buildFakeEnv();
		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, env);
		assertNotNull(fileSystem);
		fileSystem.close();
		FileSystem fileSystem2 = provider.newFileSystem(S3_GLOBAL_URI, env);
		assertNotSame(fileSystem, fileSystem2);
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void createTwoFileSystemThrowError() {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		Map<String, ?> env = buildFakeEnv();
		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, env);
		assertNotNull(fileSystem);
		provider.newFileSystem(S3_GLOBAL_URI, env);
	}

	// stream directory

	@Test
	public void createStreamDirectoryReader() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").file("file1");

		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");
		// assert
		assertNewDirectoryStream(bucket, "file1");
	}

	@Test
	public void createAnotherStreamDirectoryReader() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").file("file1", "file2");

		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");

		// assert
		assertNewDirectoryStream(bucket, "file1", "file2");
	}

	@Test
	public void createAnotherWithDirStreamDirectoryReader() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir1").file("file1");

		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");

		// assert
		assertNewDirectoryStream(bucket, "file1", "dir1");
	}

	@Test
	public void createStreamDirectoryFromDirectoryReader() throws IOException {

		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir", "dir/file2").file("dir/file1");
		// act
		Path dir = createNewS3FileSystem().getPath("/bucketA", "dir");

		// assert
		assertNewDirectoryStream(dir, "file1", "file2");
	}


	@Test(expected = UnsupportedOperationException.class)
	public void removeIteratorStreamDirectoryReader() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir1").file("dir1/file1", "content".getBytes());

		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");

		// act
		try (DirectoryStream<Path> dir = Files.newDirectoryStream(bucket)) {
			dir.iterator().remove();
		}

	}

	@Test
	public void list999Paths() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		MockBucket bucketA = client.bucket("bucketA");
		final int count999 = 999;
		for (int i = 0; i < count999; i++) {
			bucketA.file(i + "file");
		}
		Path bucket = createNewS3FileSystem().getPath("/bucketA");
		int count = 0;
		try (DirectoryStream<Path> files = Files.newDirectoryStream(bucket)) {
			Iterator<Path> iterator = files.iterator();
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}
		}
		assertEquals(count999, count);
	}

	@Test
	public void list1050Paths() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		MockBucket bucketA = client.bucket("bucketA");
		final int count1050 = 1050;
		for (int i = 0; i < count1050; i++) {
			bucketA.file(i + "file");
		}
		Path bucket = createNewS3FileSystem().getPath("/bucketA");
		int count = 0;
		try (DirectoryStream<Path> files = Files.newDirectoryStream(bucket)) {
			Iterator<Path> iterator = files.iterator();
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}
		}
		assertEquals(count1050, count);
	}

	// newInputStream

	@Test
	public void inputStreamFile() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").file("file1", "content".getBytes());

		Path file = createNewS3FileSystem().getPath("/bucketA/file1");
        try (InputStream inputStream = s3fsProvider.newInputStream(file)){

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

        try (InputStream inputStream = s3fsProvider.newInputStream(file)){

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
        try (InputStream inputStream = s3fsProvider.newInputStream(file)){
            fail("file not exists");
        }
    }

	@Test(expected = IOException.class)
	public void inputStreamDirectory() throws IOException {
		// fixtures
		Path result = getS3Directory();
		// act
		s3fsProvider.newInputStream(result);
	}

	// newOutputStream 

	@Test
	public void outputStreamFileExists() throws IOException {
		Path base = getS3Directory();

		Path file = base.resolve("file1");
		Files.createFile(file);

		final String content = "sample content";

		try (OutputStream stream = s3fsProvider.newOutputStream(file)) {
			stream.write(content.getBytes());
			stream.flush();
			stream.close();
		}
		// get the input
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}

	@Test
	public void outputStreamFileNotExists() throws IOException {
		Path base = getS3Directory();

		Path file = base.resolve("file1");

		final String content = "sample content";

		try (OutputStream stream = s3fsProvider.newOutputStream(file)) {
			stream.write(content.getBytes());
			stream.flush();
			stream.close();
		}
		// get the input
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}

	@Test
	public void outputStreamWithCreateNew() throws IOException {
		Path base = getS3Directory();

		Path file = base.resolve("file1");
		final String content = "sample content";

		try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
			stream.write(content.getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}

    @Test
    public void outputStreamWithTruncate() throws IOException {
        String initialContent = "Content line 1\n" +
                "Content line 2\n" +
                "Content line 3\n" +
                "Content line 4";
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").file("file1", initialContent.getBytes());
        Path file = createNewS3FileSystem().getPath("/bucketA/file1");

        String res = "only one line";

        try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING)) {
            stream.write(res.getBytes());
            stream.flush();
        }
        // get the input
        byte[] buffer = Files.readAllBytes(file);
        // check
        assertArrayEquals(res.getBytes(), buffer);
    }

	@Test(expected = FileAlreadyExistsException.class)
	public void outputStreamWithCreateNewAndFileExists() throws IOException {
		Path base = getS3Directory();
		Path file = Files.createFile(base.resolve("file1"));
		s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE_NEW);
	}

	@Test(expected = FileAlreadyExistsException.class)
	public void outputStreamWithCreateAndFileExists() throws IOException {
		Path base = getS3Directory();
		Path file = Files.createFile(base.resolve("file1"));
		s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE);
	}

	@Test
	public void outputStreamWithCreateAndFileNotExists() throws IOException {
		Path base = getS3Directory();

		Path file = base.resolve("file1");

		try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE)) {
			stream.write("sample content".getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals("sample content".getBytes(), buffer);
	}

	@Test
	public void anotherOutputStream() throws IOException {
		Path base = getS3Directory();
		final String content = "heyyyyyy";
		Path file = base.resolve("file1");

		try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
			stream.write(content.getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}

	// seekable

	@Test
	public void seekable() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file");

		Path base = createNewS3FileSystem().getPath("/bucketA", "dir");

		final String content = "content";

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
			seekable.write(buffer);
			ByteBuffer bufferRead = ByteBuffer.allocate(7);
			seekable.position(0);
			seekable.read(bufferRead);

			assertArrayEquals(bufferRead.array(), buffer.array());
			seekable.close();
		}

		assertArrayEquals(content.getBytes(), Files.readAllBytes(base.resolve("file")));
	}

	@Test
	public void seekableSize() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			long size = seekable.size();
			assertEquals(content.length(), size);
		}
	}

	@Test
	public void seekableAnotherSize() throws IOException {
		final String content = "content-more-large";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			long size = seekable.size();
			assertEquals(content.length(), size);
		}
	}

	@Test
	public void seekablePosition() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			long position = seekable.position();
			assertEquals(0, position);

			seekable.position(10);
			long position2 = seekable.position();
			assertEquals(10, position2);
		}
	}

	@Test
	public void seekablePositionRead() throws IOException {
		final String content = "content-more-larger";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		ByteBuffer copy = ByteBuffer.allocate(3);
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.READ))) {
			long position = seekable.position();
			assertEquals(0, position);

			seekable.read(copy);
			long position2 = seekable.position();
			assertEquals(3, position2);
		}
	}

	@Test
	public void seekablePositionWrite() throws IOException {
		final String content = "content-more-larger";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		ByteBuffer copy = ByteBuffer.allocate(5);
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE))) {
			long position = seekable.position();
			assertEquals(0, position);

			seekable.write(copy);
			long position2 = seekable.position();
			assertEquals(5, position2);
		}
	}

	@Test
	public void seekableIsOpen() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE))) {
			assertTrue(seekable.isOpen());
		}
		SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.READ));
		assertTrue(seekable.isOpen());
		seekable.close();
		assertTrue(!seekable.isOpen());
	}

	@Test
	public void seekableRead() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		final String content = "content";
		Path file = Files.write(base.resolve("file"), content.getBytes());
		ByteBuffer bufferRead = ByteBuffer.allocate(7);
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.READ))) {
			seekable.position(0);
			seekable.read(bufferRead);
		}
		assertArrayEquals(bufferRead.array(), content.getBytes());
		assertArrayEquals(content.getBytes(), Files.readAllBytes(base.resolve("file")));
	}

	@Test
	public void seekableReadPartialContent() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		final String content = "content";
		Path file = Files.write(base.resolve("file"), content.getBytes());
		ByteBuffer bufferRead = ByteBuffer.allocate(4);
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.READ))) {
			seekable.position(3);
			seekable.read(bufferRead);
		}
		assertArrayEquals(bufferRead.array(), "tent".getBytes());
		assertArrayEquals("content".getBytes(), Files.readAllBytes(base.resolve("file")));
	}

	@Test
	public void seekableTruncate() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			// discard all content except the first c.
			seekable.truncate(1);
		}
		assertArrayEquals("c".getBytes(), Files.readAllBytes(file));
	}

	@Test
	public void seekableAnotherTruncate() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			// discard all content except the first three chars 'con'
			seekable.truncate(3);
		}
		assertArrayEquals("con".getBytes(), Files.readAllBytes(file));
	}

	@Test
	public void seekableruncateGreatherThanSize() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			seekable.truncate(10);
		}
		assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
	}

	@Test
	public void seekableCreateEmpty() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		Path file = base.resolve("file");
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.CREATE))) {
			//
		}
		assertTrue(Files.exists(file));
		assertArrayEquals("".getBytes(), Files.readAllBytes(file));
	}

	@Test
	public void seekableDeleteOnClose() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		Path file = Files.createFile(base.resolve("file"));
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.DELETE_ON_CLOSE))) {
			seekable.close();
		}
		assertTrue(Files.notExists(file));
	}

	@Test
	public void seekableCloseTwice() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		Path file = Files.createFile(base.resolve("file"));
		SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.noneOf(StandardOpenOption.class));
		seekable.close();
		seekable.close();
		assertTrue(Files.exists(file));
	}

	@Test
	public void createDirectory() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA");

		// act
		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		Files.createDirectory(base);
		// assert
		assertTrue(Files.exists(base));
		assertTrue(Files.isDirectory(base));
		assertTrue(Files.exists(base));
	}


    @Test
    public void createDirectoryInNewBucket() throws IOException {
        S3Path root = createNewS3FileSystem().getPath("/newer-bucket");
        Path resolve = root.resolve("folder");
        Path path = Files.createDirectories(resolve);
        assertEquals("/newer-bucket/folder", path.toAbsolutePath().toString());
        // assert
        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.exists(root));
        assertTrue(Files.exists(resolve));
        assertTrue(Files.isDirectory(resolve));
        assertTrue(Files.exists(resolve));
    }

    @Test
    public void createDirectoryWithSpace() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA");

        // act
        Path base = createNewS3FileSystem().getPath("/bucketA/dir with space/another space");
        Files.createDirectories(base);
        // assert
        assertTrue(Files.exists(base));
        assertTrue(Files.isDirectory(base));
        // parent
        assertTrue(Files.exists(base.getParent()));
        assertTrue(Files.isDirectory(base.getParent()));
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
		Path base = getS3Directory();
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

	// copy

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

	@Test(expected = UnsupportedOperationException.class)
	public void move() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir","dir2").file("dir/file1");
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file = fs.getPath("/bucketA/dir/file1");
		Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
		s3fsProvider.move(file, fileDest);
	}

	@Test
	public void isSameFileTrue() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file1");
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file1");
		Path fileCopy = fs.getPath("/bucketA/dir/file1");
		// assert
		assertTrue(s3fsProvider.isSameFile(file1, fileCopy));
	}

	@Test
	public void isSameFileFalse() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir", "dir2").file("dir/file1", "dir2/file13");
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file1");
		Path fileCopy = fs.getPath("/bucketA/dir2/file2");
		// assert
		assertTrue(!s3fsProvider.isSameFile(file1, fileCopy));
	}

	// isHidden

	@Test
	public void isHidden() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file1");
		// act
		Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");
		// assert
		assertTrue(!s3fsProvider.isHidden(file1));
	}

	// getFileStore

	@Test(expected = UnsupportedOperationException.class)
	public void getFileStore() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file1");

		// act
		Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");
		// assert
		s3fsProvider.getFileStore(file1);
	}

	// getFileAttributeView

	@Test(expected = UnsupportedOperationException.class)
	public void getFileAttributeView() {
		s3fsProvider.getFileAttributeView(null, null);
	}

	// readAttributes

	@Test
	public void readAttributesFileEmpty() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir").file("dir/file1");

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

	@Test(expected = UnsupportedOperationException.class)
	public void readAttributesString() throws IOException {
		s3fsProvider.readAttributes(null, "");
	}

	// setAttribute

	@Test(expected = UnsupportedOperationException.class)
	public void readAttributesObject() throws IOException {
		s3fsProvider.setAttribute(null, "", new Object());
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
		//S3AccessControlList acl = file1.getFileStore().getAccessControlList(file1);
		//assertEquals("dir/file", acl.getKey());
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

	private Map<String, ?> buildFakeEnv() {
		return ImmutableMap.<String, Object> builder().put(ACCESS_KEY, "access key").put(SECRET_KEY, "secret key").build();
	}

	private Properties buildFakeProps(String access_key, String secret_key, String encoding) {
		Properties props = buildFakeProps(access_key, secret_key);
		props.setProperty(CHARSET_KEY, encoding);
		return props;
	}

	private Properties buildFakeProps(String access_key, String secret_key) {
		Properties props = new Properties();
		props.setProperty(AMAZON_S3_FACTORY_CLASS, "com.upplication.s3fs.util.AmazonS3MockFactory");
		if (access_key != null)
			props.setProperty(ACCESS_KEY, access_key);
		if (secret_key != null)
			props.setProperty(SECRET_KEY, secret_key);
		return props;
	}

	private void assertNewDirectoryStream(Path base, final String... files) throws IOException {
		try (DirectoryStream<Path> dir = Files.newDirectoryStream(base)) {
			assertNotNull(dir);
			assertNotNull(dir.iterator());
			assertTrue(dir.iterator().hasNext());

			Set<String> filesNamesExpected = new HashSet<>(Arrays.asList(files));
			Set<String> filesNamesActual = new HashSet<>();

			for (Path path : dir) {
				String fileName = path.getFileName().toString();
				filesNamesActual.add(fileName);
			}

			assertEquals(filesNamesExpected, filesNamesActual);
		}
	}

	private Path getS3Directory() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir");
		return s3fsProvider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
	}

	/**
	 * create a new file system for s3 scheme with fake credentials
	 * and global endpoint
	 * @return FileSystem
	 * @throws IOException
	 */
	private S3FileSystem createNewS3FileSystem() throws IOException {
        try {
            return s3fsProvider.getFileSystem(S3_GLOBAL_URI);
        }
        catch (FileSystemNotFoundException e){
            return (S3FileSystem) FileSystems.newFileSystem(S3_GLOBAL_URI, null);
        }

	}
}