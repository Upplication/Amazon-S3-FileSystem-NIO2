package com.upplication.s3fs;

import com.upplication.s3fs.util.EnvironmentBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import java.nio.file.FileSystemNotFoundException;
import static org.junit.Assert.*;

public class FileSystemsIT {

    private static final URI uriEurope = URI.create("s3://s3-eu-west-1.amazonaws.com/");
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
    public void buildEnv() {
        FileSystem fileSystem = FileSystems.getFileSystem(uriGlobal);
        assertSame(fileSystemAmazon, fileSystem);
    }

    @Test
    public void buildEnvAnotherURIReturnDifferent() throws IOException {
        FileSystem fileSystem = FileSystems.newFileSystem(uriEurope, null);
        assertNotSame(fileSystemAmazon, fileSystem);
    }
}
