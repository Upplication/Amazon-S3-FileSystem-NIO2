package com.upplication.s3fs.util;


import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.junit.Ignore;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3Path;

@Ignore
public class S3ObjectSummaryLookupIT {
	public static void main(String[] args) throws Exception {
		S3ObjectSummaryLookupIT it = new S3ObjectSummaryLookupIT();
		it.runTests();
	}
	
    private void runTests() throws Exception {
    	setup();
    	lookup_S3Object_when_S3Path_is_file();
    	lookup_S3Object_when_S3Path_is_file_and_exists_other_starts_with_same_name();
    	lookup_S3Object_when_S3Path_is_a_directory();
    	lookup_S3Object_when_S3Path_is_a_directory_and_exists_other_directory_starts_same_name();
    	lookup_S3Object_when_S3Path_is_a_directory_and_is_virtual();
	}
    
	private static final URI uri = URI.create("s3:///");
    private static final String bucket = EnvironmentBuilder.getBucket();

    private FileSystem fileSystemAmazon;
    private S3ObjectSummaryLookup s3ObjectSummaryLookup;

    public void setup() throws IOException{
        fileSystemAmazon = build();
        s3ObjectSummaryLookup = new S3ObjectSummaryLookup();
    }

    private static FileSystem build() throws IOException{
        try {
            FileSystems.getFileSystem(uri).close();
            return createNewFileSystem();
        } catch(FileSystemNotFoundException e){
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem() throws IOException {
        return FileSystems.newFileSystem(uri, EnvironmentBuilder.getRealEnv());
    }

    public void lookup_S3Object_when_S3Path_is_file() throws IOException {

        Path path;
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")){

            Path base = Files.createDirectory(linux.getPath("/base"));
            Files.createFile(base.resolve("file"));
            path = fileSystemAmazon.getPath(bucket, startPath);
            Files.walkFileTree(base, new CopyDirVisitor(base, path));
        }

        S3Path s3Path = (S3Path) path.resolve("file");
        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(s3Path.getKey(), result.getKey());
    }

    public void lookup_S3Object_when_S3Path_is_file_and_exists_other_starts_with_same_name() throws IOException {
        Path path;
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")){

            Path base = Files.createDirectory(linux.getPath("/base"));
            Files.createFile(base.resolve("file"));
            Files.createFile(base.resolve("file1"));
            path = fileSystemAmazon.getPath(bucket, startPath);
            Files.walkFileTree(base, new CopyDirVisitor(base, path));
        }

        S3Path s3Path = (S3Path) path.resolve("file");
        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(s3Path.getKey(), result.getKey());
    }

    public void lookup_S3Object_when_S3Path_is_a_directory() throws IOException {
        Path path;
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")){

            Path base = Files.createDirectories(linux.getPath("/base").resolve("dir"));
            path = fileSystemAmazon.getPath(bucket, startPath);
            Files.walkFileTree(base.getParent(), new CopyDirVisitor(base.getParent(), path));
        }

        S3Path s3Path = (S3Path) path.resolve("dir");

        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(s3Path.getKey() + "/", result.getKey());
    }

    public void lookup_S3Object_when_S3Path_is_a_directory_and_exists_other_directory_starts_same_name() throws IOException {

        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        S3FileSystem s3FileSystem = (S3FileSystem)fileSystemAmazon;
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0L);
        s3FileSystem.getClient().putObject(bucket.replace("/",""), startPath + "lib/angular/", new ByteArrayInputStream("".getBytes()), metadata);
        s3FileSystem.getClient().putObject(bucket.replace("/",""), startPath + "lib/angular-dynamic-locale/", new ByteArrayInputStream("".getBytes()), metadata);


        S3Path s3Path = (S3Path) s3FileSystem.getPath(bucket, startPath, "lib", "angular");
        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(startPath + "lib/angular/", result.getKey());
    }

    public void lookup_S3Object_when_S3Path_is_a_directory_and_is_virtual() throws IOException {

        String folder = "angular" + UUID.randomUUID().toString();
        String key = folder + "/content.js";

        S3FileSystem s3FileSystem = (S3FileSystem)fileSystemAmazon;
        s3FileSystem.getClient().putObject(bucket.replace("/",""), key, new ByteArrayInputStream("contenido1".getBytes()), new ObjectMetadata());

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(bucket, folder);
        S3ObjectSummary result = s3ObjectSummaryLookup.lookup(s3Path);

        assertEquals(key, result.getKey());
    }
}
