package com.upplication.s3fs.Path;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.*;

public class EqualsTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    private S3Path getPath(String path) {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void equals() {
        Path path = getPath("/bucketA/dir/file");
        Path path2 = getPath("/bucketA/dir/file");

        assertEquals(path, path2);
    }

    @Test
    public void equalsDir() {
        Path path = getPath("/bucketA/dir/");
        Path path2 = getPath("/bucketA/dir/");

        assertEquals(path, path2);
    }

    @Test
    public void equalsBucket() {
        Path path = getPath("/bucketA/");
        Path path2 = getPath("/bucketA/");

        assertEquals(path, path2);
    }

    @Test
    public void equalsBucketWithoutEndSlash() {
        Path path = getPath("/bucketA/");
        Path path2 = getPath("/bucketA");

        assertNotEquals(path, path2);
    }

    @Test
    public void notEquals() {
        Path path = getPath("/bucketA/dir/file");
        Path path2 = getPath("/bucketA/dir/file2");

        assertNotEquals(path, path2);
    }

    @Test
    public void notEqualsDirFile() {
        Path path = getPath("/bucketA/dir/asd/");
        Path path2 = getPath("/bucketA/dir/asd");

        assertNotEquals(path, path2);
    }

    @Test
    public void notEqualsNull() {
        Path path = getPath("/bucketA/dir/file");

        assertNotEquals(path, null);
    }

    @Test
    public void notEqualsDifferentProvider() throws IOException {
        Path path = getPath("/c/dir/file");

        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")) {
            Path fileLinux = linux.getPath("/dir/file");

            assertNotEquals(path, fileLinux);
        }

        try (FileSystem window = MemoryFileSystemBuilder.newWindows().build("window")) {
            Path file = window.getPath("c:/dir/file");

            assertNotEquals(path, file);
        }

        Path pathS3EmptyEndpoint = FileSystems.newFileSystem(URI.create("s3:///"), null).getPath("/bucketA/dir/");
        Path pathS3TestEndpoint = getPath("/bucketA/dir/");

        assertNotEquals(pathS3EmptyEndpoint, pathS3TestEndpoint);
    }

}
