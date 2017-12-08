package com.upplication.s3fs.spike;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;

public class FileStoreTest {

    FileSystem fs;
    FileSystem fsWindows;
    FileSystem fsMac;

    @Before
    public void setup() throws IOException {
        fs = MemoryFileSystemBuilder.newLinux().build("linux");
        fsWindows = MemoryFileSystemBuilder.newWindows().build("windows");
        fsMac = MemoryFileSystemBuilder.newMacOs().build("mac");
    }

    @After
    public void close() throws IOException {
        fs.close();
        fsWindows.close();
        fsMac.close();
    }

    @Test
    public void getFileStore() {
        System.out.println("Default:");
        System.out.println("-------");
        for (FileStore fileStore : FileSystems.getDefault().getFileStores()) {
            System.out.println("- " + fileStore.name());
        }
        System.out.println("\nLinux:");
        System.out.println("-----");
        for (FileStore fileStore : fs.getFileStores()) {
            System.out.println("- " + fileStore.name());
        }
        System.out.println("\nWindows:");
        System.out.println("-------");
        for (FileStore fileStore : fsWindows.getFileStores()) {
            System.out.println("- " + fileStore.name());
        }
        System.out.println("\nMac:");
        System.out.println("---");
        for (FileStore fileStore : fsMac.getFileStores()) {
            System.out.println("- " + fileStore.name());
        }
    }


    // ~ helpers methods

    private Path get(String path) {
        return fs.getPath(path);
    }
}
