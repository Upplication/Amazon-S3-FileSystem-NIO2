package com.upplication.s3fs;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectId;
import java.io.IOException;

import java.io.OutputStream;
import java.util.Properties;

import static com.upplication.s3fs.S3FileSystemProvider.MULTIPART_UPLOAD_NUM_UPLOAD_THREADS;
import static com.upplication.s3fs.S3FileSystemProvider.MULTIPART_UPLOAD_QUEUE_CAPACITY;
import static com.upplication.s3fs.S3FileSystemProvider.MULTIPART_UPLOAD_NUM_STREAMS;
import static com.upplication.s3fs.S3FileSystemProvider.MULTIPART_UPLOAD_PART_SIZE;

public class S3MultipartUploadOutputStream extends OutputStream {

    private final MultiPartOutputStream outputStream;

    private final StreamTransferManager manager;

    private static final int DEFAULT_NUM_UPLOAD_THREADS = 1;

    private static final int DEFAULT_QUEUE_CAPACITY = 1;

    private static final int DEFAULT_NUM_STREAMS = 1;

    private static final int DEFAULT_PART_SIZE = 5;

    public S3MultipartUploadOutputStream(final AmazonS3 s3Client, final S3ObjectId objectId, final Properties properties) {
        this(
            createStreamTransferManager(s3Client, objectId, properties),
            objectId
        );
    }

    public S3MultipartUploadOutputStream(
        final StreamTransferManager manager,
        final S3ObjectId objectId) {

        this.manager = manager;
        this.outputStream = manager.getMultiPartOutputStreams().get(0);
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);

        try {
            outputStream.checkSize();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.outputStream.close();
        this.manager.complete();
    }

    private static StreamTransferManager createStreamTransferManager(
        final AmazonS3 s3Client,
        final S3ObjectId objectId,
        final Properties properties) {

        return new StreamTransferManager(
            objectId.getBucket(),
            objectId.getKey(),
            s3Client,
            getIntValue(properties, MULTIPART_UPLOAD_NUM_STREAMS, DEFAULT_NUM_STREAMS),
            getIntValue(properties, MULTIPART_UPLOAD_NUM_UPLOAD_THREADS, DEFAULT_NUM_UPLOAD_THREADS),
            getIntValue(properties, MULTIPART_UPLOAD_QUEUE_CAPACITY, DEFAULT_QUEUE_CAPACITY),
            getIntValue(properties, MULTIPART_UPLOAD_PART_SIZE, DEFAULT_PART_SIZE)
        );
    }

    private static int getIntValue(final Properties properties, final String name, final int defaultValue) {
        return Integer.parseInt(properties.getProperty(name, String.valueOf(defaultValue)));
    }
}