package com.upplication.s3fs.FileSystemProvider;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.EnumSet;
import java.util.UUID;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.Assert.*;

public class NewByteChannelIT extends S3UnitTestBase {

    private static final String bucket = EnvironmentBuilder.getBucket();
    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3_GLOBAL_URI_IT);

    private FileSystem fileSystemAmazon;

    @Before
    public void setup() throws IOException {
        System.clearProperty(S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS);
        fileSystemAmazon = build();
    }

    private static FileSystem build() throws IOException {
        try {
            FileSystems.getFileSystem(uriGlobal).close();
            return createNewFileSystem();
        } catch (FileSystemNotFoundException e) {
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem() throws IOException {
        return FileSystems.newFileSystem(uriGlobal, EnvironmentBuilder.getRealEnv());
    }

    @Test
    public void newByteChannelCreateAndWrite() throws IOException {
        final String content = "sample content";
        Path file = uploadDir().resolve("file");

        try (SeekableByteChannel seek =
                     fileSystemAmazon.provider().newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW))){
            ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
            seek.write(buffer);
        }

        assertTrue(Files.exists(file));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
    }

    @Test
    public void newByteChannelWrite() throws IOException {
        final String content = "sample content";
        Path file = uploadSingleFile("");

        try (SeekableByteChannel seek =
                     fileSystemAmazon.provider().newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){
            seek.write(ByteBuffer.wrap(content.getBytes()));
        }

        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
    }

    @Test(expected = NonWritableChannelException.class)
    public void newByteChannelWriteWithoutPermission() throws IOException {
        final String content = "sample content";
        Path file = uploadSingleFile("");

        try (SeekableByteChannel seek =
                     fileSystemAmazon.provider().newByteChannel(file, EnumSet.of(StandardOpenOption.READ))){
            seek.write(ByteBuffer.wrap(content.getBytes()));
        }
    }

    @Test
    public void newByteChannelRead() throws IOException {
        final String content = "sample content";
        Path file = uploadSingleFile(content);
        ByteBuffer bufferRead = ByteBuffer.allocate(content.length());

        try (SeekableByteChannel seek =
                     fileSystemAmazon.provider().newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){
            seek.read(bufferRead);
        }

        assertArrayEquals(new String(bufferRead.array()).getBytes(), Files.readAllBytes(file));
        assertArrayEquals(new String(bufferRead.array()).getBytes(), content.getBytes());
    }

    @Test(expected = NonReadableChannelException.class)
    public void newByteChannelReadWithoutPermission() throws IOException {
        final String content = "sample content";
        Path file = uploadSingleFile(content);

        try (SeekableByteChannel seek =
                     fileSystemAmazon.provider().newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE))){
            seek.read(ByteBuffer.allocate(content.length()));
        }
    }

    private Path uploadSingleFile(String content) throws IOException {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")) {

            Path file = Files.createFile(linux.getPath(UUID.randomUUID().toString()));
            Files.write(file, content.getBytes());

            Path result = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());

            Files.copy(file, result);
            return result;
        }
    }

    private Path uploadDir() throws IOException {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")) {
            Path assets = Files.createDirectories(linux.getPath("/upload/assets1"));
            Path dir = fileSystemAmazon.getPath(bucket, "0000example" + UUID.randomUUID().toString() + "/");
            Files.walkFileTree(assets.getParent(), new CopyDirVisitor(assets.getParent(), dir));
            return dir;
        }
    }
}