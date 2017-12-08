package com.upplication.s3fs.util;

import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.util.Properties;
import java.util.UUID;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3;
import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.AmazonS3Factory;

public class AmazonS3MockFactory extends AmazonS3Factory {

    private static FileSystem fsMem;
    private static AmazonS3ClientMock amazonS3Client;

    @Override
    public AmazonS3 getAmazonS3(URI uri, Properties props) {
        return getAmazonClientMock();
    }

    @Override
    protected AmazonS3 createAmazonS3(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration, RequestMetricCollector requestMetricsCollector) {
        return getAmazonClientMock();
    }

    public static AmazonS3ClientMock getAmazonClientMock() {
        if (amazonS3Client == null)
            amazonS3Client = spy(new AmazonS3ClientMock(getFsMem().getPath("/")));
        return amazonS3Client;
    }

    private static FileSystem getFsMem() {
        if (fsMem == null)
            try {
                fsMem = MemoryFileSystemBuilder.newLinux()
                        .setCurrentWorkingDirectory("/")
                        .build(UUID.randomUUID().toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        return fsMem;
    }
}