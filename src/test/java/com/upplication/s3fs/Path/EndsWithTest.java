package com.upplication.s3fs.Path;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EndsWithTest extends S3UnitTestBase {

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
    public void endsWithAbsoluteRelative() {
        assertTrue(getPath("/bucket/file1").endsWith(getPath("file1")));
    }

    @Test
    public void endsWithAbsoluteAbsolute() {
        assertTrue(getPath("/bucket/file1").endsWith(getPath("/bucket/file1")));
    }

    @Test
    public void endsWithRelativeRelative() {
        assertTrue(getPath("file/file1").endsWith(getPath("file1")));
    }

    @Test
    public void endsWithRelativeAbsolute() {
        assertFalse(getPath("file/file1").endsWith(getPath("/bucket")));
    }

    @Test
    public void endsWithDifferenteFileSystem() {
        assertFalse(getPath("/bucket/file1").endsWith(Paths.get("/bucket/file1")));
    }

    @Test
    public void endsWithBlankRelativeAbsolute() {
        assertFalse(getPath("").endsWith(getPath("/bucket")));
    }

    @Test
    public void endsWithBlankBlank() {
        assertTrue(getPath("").endsWith(getPath("")));
    }

    @Test
    public void endsWithRelativeBlankAbsolute() {
        assertFalse(getPath("/bucket/file1").endsWith(getPath("")));
    }

    @Test
    public void endsWithRelativeBlankRelative() {
        assertFalse(getPath("file1").endsWith(getPath("")));
    }

    @Test
    public void endsWithDifferent() {
        assertFalse(getPath("/bucket/dir/dir/file1").endsWith(getPath("fail/dir/file1")));
    }

    @Test
    public void endsWithDifferentProvider() throws IOException {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")) {
            Path fileLinux = linux.getPath("/file");

            assertFalse(getPath("/bucket/file").endsWith(fileLinux));
        }

        try (FileSystem window = MemoryFileSystemBuilder.newWindows().build("window")) {
            Path file = window.getPath("c:/file");

            assertFalse(getPath("/c/file").endsWith(file));
        }
    }

    @Test
    public void endsWithString() {
        // endsWithAbsoluteRelative(){
        assertTrue(getPath("/bucket/file1").endsWith("file1"));
        // endsWithAbsoluteAbsolute
        assertTrue(getPath("/bucket/file1").endsWith("/bucket/file1"));
        // endsWithRelativeRelative
        assertTrue(getPath("file/file1").endsWith("file1"));
        // endsWithRelativeAbsolute
        assertFalse(getPath("file/file1").endsWith("/bucket"));
        // endsWithBlankRelativeAbsolute
        assertFalse(getPath("").endsWith("/bucket"));
        // endsWithBlankBlank
        assertTrue(getPath("").endsWith(""));
        // endsWithRelativeBlankAbsolute
        assertFalse(getPath("/bucket/file1").endsWith(""));
        // endsWithRelativeBlankRelative
        assertFalse(getPath("file1").endsWith(""));
    }
}
