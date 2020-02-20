package com.upplication.s3fs;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.util.EnvironmentBuilder;

public class FilesServerSideEncryptIT  {

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
    	Map<String, Object> env = new HashMap<String, Object>();
    	env.putAll(EnvironmentBuilder.getRealEnv());
    	env.put(AmazonS3Factory.ENCRYPT_SSE_S3, "true");
    	
        return FileSystems.newFileSystem(uriGlobal, env);
    }

    @Test
    public void amazonCopyVerifyEncryption() throws IOException {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")) {
            Path htmlFile = Files.write(linux.getPath("/index.html"), "<html><body>html file</body></html>".getBytes());

            Path result = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString() + htmlFile.getFileName().toString());
            Files.copy(htmlFile, result);

            S3Path resultS3 = (S3Path) result;
            ObjectMetadata metadata = resultS3.getFileSystem().getClient().getObjectMetadata(resultS3.getFileStore().name(), resultS3.getKey());
            assertEquals(metadata.getRawMetadataValue(Headers.SERVER_SIDE_ENCRYPTION), SSEAlgorithm.getDefault().name());
        }
    }
}
