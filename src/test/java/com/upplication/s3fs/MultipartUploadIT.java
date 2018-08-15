package com.upplication.s3fs;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.util.EnvironmentBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import static org.junit.Assert.*;

public class MultipartUploadIT {
    private static final String BUCKET = EnvironmentBuilder.getBucket();
    private static final URI S3_URI = EnvironmentBuilder.getS3URI(S3_GLOBAL_URI_IT);

    private FileSystem fileSystem;

    @Before
    public void setup() throws IOException {
        System.clearProperty(S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS);

        this.fileSystem = build();
    }

    private static FileSystem build() throws IOException {
        try {
            FileSystems.getFileSystem(S3_URI).close();
            return createNewFileSystem();
        } catch (FileSystemNotFoundException e) {
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem() throws IOException {
        return FileSystems.newFileSystem(
            S3_URI,
            ImmutableMap.<String, Object>builder()
                .put(S3FileSystemProvider.MULTIPART_UPLOAD_ENABLED, "true")
                .putAll(EnvironmentBuilder.getRealEnv())
                .build()
        );
    }

    @Test
    public void testUploadUsingByteChannel() throws IOException {
        final Path path = fileSystem.getPath(BUCKET, UUID.randomUUID().toString());
        final byte[] bytes = randomMegaBytes(5);

        Files.write(path, bytes, StandardOpenOption.CREATE_NEW);

        assertTrue(Files.exists(path));
    }

    @Test
    public void testUploadUsingFileChannel() throws IOException {
        final byte[] bytes = randomMegaBytes(10);
        final Path path = fileSystem.getPath(BUCKET, UUID.randomUUID().toString());

        try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes));
        }

        assertTrue(Files.exists(path));
    }

    private byte[] randomMegaBytes(final int sizeInMegabytes) throws IOException {
        final byte[] bytes = new byte[sizeInMegabytes * 1024 * 1024];
        final SecureRandom random = new SecureRandom();

        random.nextBytes(bytes);

        return bytes;
    }
}
