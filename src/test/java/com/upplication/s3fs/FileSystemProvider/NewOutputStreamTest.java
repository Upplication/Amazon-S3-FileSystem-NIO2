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
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.*;
import java.util.Properties;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class NewOutputStreamTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
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

    @Test
    public void outputStreamWithCreateAndFileExists() throws IOException {
        Path base = getS3Directory();

        Path file = base.resolve("file1");
        Files.createFile(file);

        final String content = "sample content";

        try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE)) {
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

    private Path getS3Directory() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");
        return s3fsProvider.newFileSystem(URI.create("s3://endpoint1/"), ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access_key").put(SECRET_KEY, "secret_key").build()).getPath("/bucketA/dir");
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