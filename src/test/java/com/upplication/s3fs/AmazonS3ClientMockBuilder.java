package com.upplication.s3fs;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import com.upplication.s3fs.util.AmazonS3ClientMock;


public class AmazonS3ClientMockBuilder {

    private FileSystem fs;
    private Path bucket;

    public AmazonS3ClientMockBuilder(FileSystem fs){
        this.fs = fs;
    }

    /**
     * create the base bucket
     * @param bucket
     * @return
     */
    public AmazonS3ClientMockBuilder withBucket(String bucket){
        try {
            this.bucket = Files.createDirectories(fs.getPath("/" + bucket));
        }
        catch (IOException e) {
           throw new IllegalStateException(e);
        }
        return this;
    }

    /**
     * add directories to the bucket
     * @param dir String path with optional '/' separator for subdirectories
     * @return
     */
    public AmazonS3ClientMockBuilder withDirectory(String dir){
        try {
            Files.createDirectories(this.bucket.resolve(dir));
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    /**
     * add a file with content to the bucket
     * @param file String path with optional '/' separator for directories
     * @param content
     * @return
     */
    public AmazonS3ClientMockBuilder withFile(String file, String content){
        try {
            Path filepath = prepareFile(file);

            Files.write(filepath, content.getBytes());
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    /**
     * add a empty file to the bucket
     * @param file
     * @return
     */
    public AmazonS3ClientMockBuilder withFile(String file){
        try {
            Path filepath = prepareFile(file);
            Files.createFile(filepath);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    /**
     * build and associate the AmazonS3Client to the S3Provider
     * @param provider S3FileSystemProvider
     * @return AmazonS3ClientMock and ready to stub with mockito.
     */
    public AmazonS3ClientMock build(S3FileSystemProvider provider){
        try {
            AmazonS3ClientMock clientMock = spy(new AmazonS3ClientMock(fs.getPath("/")));
            S3FileSystem s3ileS3FileSystem = new S3FileSystem(provider, clientMock, "endpoint");
            doReturn(s3ileS3FileSystem).when(provider).createFileSystem(any(URI.class), (Properties) anyObject());
            return clientMock;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Path prepareFile(String file) throws IOException {
        Path filepath = this.bucket.resolve(file);
        if (filepath.getParent() != null){
            Files.createDirectories(filepath.getParent());
        }
        return filepath;
    }

}
