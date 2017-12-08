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
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.EnumSet;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class NewByteChannelTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void seekable() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file", "".getBytes());

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
        final String content = "content";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

        ByteBuffer bufferRead = ByteBuffer.allocate(7);
        try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.READ))) {
            seekable.position(0);
            seekable.read(bufferRead);
        }
        assertArrayEquals(bufferRead.array(), content.getBytes());
        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
    }

    @Test
    public void seekableReadPartialContent() throws IOException {
        // fixtures
        final String content = "content";
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

        ByteBuffer bufferRead = ByteBuffer.allocate(4);
        try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.READ))) {
            seekable.position(3);
            seekable.read(bufferRead);
        }
        assertArrayEquals(bufferRead.array(), "tent".getBytes());
        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
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

    @Test(expected = NoSuchFileException.class)
    public void seekableNotExists() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");

        Path base = createNewS3FileSystem().getPath("/bucketA", "dir");
        s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.noneOf(StandardOpenOption.class));
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