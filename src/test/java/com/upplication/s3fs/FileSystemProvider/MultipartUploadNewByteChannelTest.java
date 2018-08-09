package com.upplication.s3fs.FileSystemProvider;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3MultipartUploadChannel;
import com.upplication.s3fs.S3SeekableByteChannel;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.spi.FileSystemProvider;
import java.security.SecureRandom;
import java.util.EnumSet;

import static org.junit.Assert.*;

public class MultipartUploadNewByteChannelTest extends S3UnitTestBase {

    @Test
    public void testNewByteChannelMultipartUpload() throws IOException, InterruptedException {
        final byte[] bytes = randomMegaBytes(1);
        final FileSystem fileSystem = createFileSystem(true);
        final FileSystemProvider provider = fileSystem.provider();
        final Path path = fileSystem.getPath("/test-bucket/test-path");
        final AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();

        client.bucket("test-bucket");

        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final SeekableByteChannel channel = provider.newByteChannel(path, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE));

        channel.write(buffer);
        channel.close();

        assertEquals(S3MultipartUploadChannel.class, channel.getClass());
        assertEquals(bytes.length, channel.size());
    }

    @Test
    public void testUseDefaultChannelForReading() throws IOException, InterruptedException {
        final FileSystem fileSystem = createFileSystem(true);
        final FileSystemProvider provider = fileSystem.provider();
        final Path path = fileSystem.getPath("/test-bucket/test-path");
        final AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();

        client.bucket("test-bucket").file("test-path");

        final SeekableByteChannel channel = provider.newByteChannel(path, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ));

        channel.close();

        assertEquals(S3SeekableByteChannel.class, channel.getClass());
    }

    @Test
    public void testUseDefaultChannelWhenDisabled() throws IOException, InterruptedException {
        final FileSystem fileSystem = createFileSystem(false);
        final FileSystemProvider provider = fileSystem.provider();
        final Path path = fileSystem.getPath("/test-bucket/test-path");
        final AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();

        client.bucket("test-bucket");

        final SeekableByteChannel channel = provider.newByteChannel(path, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE));

        assertEquals(S3SeekableByteChannel.class, channel.getClass());
    }

    private byte[] randomMegaBytes(final int sizeInMegabytes) throws IOException {
        final byte[] bytes = new byte[sizeInMegabytes * 1024 * 1024];
        final SecureRandom random = new SecureRandom();

        random.nextBytes(bytes);

        return bytes;
    }

    private FileSystem createFileSystem(final boolean multipartEnabled) throws IOException {
        final S3FileSystemProvider provider = getS3fsProvider();
        final FileSystem fileSystem = provider.newFileSystem(
            S3EndpointConstant.S3_GLOBAL_URI_TEST,
            ImmutableMap.of(S3FileSystemProvider.MULTIPART_UPLOAD_ENABLED, String.valueOf(multipartEnabled))
        );

        return fileSystem;
    }
}