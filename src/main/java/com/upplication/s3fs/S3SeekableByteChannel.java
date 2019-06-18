package com.upplication.s3fs;

import static java.lang.String.format;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import com.amazonaws.services.s3.model.S3Object;
import com.upplication.s3fs.attribute.S3BasicFileAttributeView;

public class S3SeekableByteChannel implements SeekableByteChannel {

    private S3Path path;
    private Set<? extends OpenOption> options;
    private SeekableByteChannel seekable;
    private Path tempFile;
    private PutObjectResult putResult;
    private Date lastModified;

    /**
     * Open or creates a file, returning a seekable byte channel
     *
     * @param path    the path open or create
     * @param options options specifying how the file is opened
     * @throws IOException if an I/O error occurs
     */
    public S3SeekableByteChannel(S3Path path, Set<? extends OpenOption> options) throws IOException {
        this.path = path;
        this.options = Collections.unmodifiableSet(new HashSet<>(options));
        String key = path.getKey();

        boolean exists = false;
        if (!options.contains(StandardOpenOption.TRUNCATE_EXISTING)) {
            exists = path.getFileSystem().provider().exists(path);

            if (exists && this.options.contains(StandardOpenOption.CREATE_NEW))
                throw new FileAlreadyExistsException(format("target already exists: %s", path));
            else if (!exists && !this.options.contains(StandardOpenOption.CREATE_NEW) &&
                    !this.options.contains(StandardOpenOption.CREATE))
                throw new NoSuchFileException(format("target not exists: %s", path));
        }

        tempFile = Files.createTempFile("temp-s3-", String.valueOf(System.nanoTime()));
        boolean removeTempFile = true;
        try {
            if (exists) {
                try (S3Object object = path.getFileSystem()
                        .getClient()
                        .getObject(path.getFileStore().getBucket().getName(), key)) {
                    Files.copy(object.getObjectContent(), tempFile, StandardCopyOption.REPLACE_EXISTING);
                }
            }

            Set<? extends OpenOption> seekOptions = new HashSet<>(this.options);
            seekOptions.remove(StandardOpenOption.CREATE_NEW);
            seekable = Files.newByteChannel(tempFile, seekOptions);
            removeTempFile = false;
        } finally {
            if (removeTempFile) {
                Files.deleteIfExists(tempFile);
            }
        }
    }

    @Override
    public boolean isOpen() {
        return seekable.isOpen();
    }

    @Override
    public void close() throws IOException {
        try {
            if (!seekable.isOpen())
                return;

            seekable.close();

            if (options.contains(StandardOpenOption.DELETE_ON_CLOSE)) {
                path.getFileSystem().provider().delete(path);
                return;
            }

            if (options.contains(StandardOpenOption.READ) && options.size() == 1) {
                return;
            }

            sync();

        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private static final ExecutorFactory EXECUTOR_FACTORY = new ExecutorFactory() {
        @Override
        public ExecutorService newExecutor() {
            return Executors.newFixedThreadPool(3);
        }
    };

    /**
     * try to sync the temp file with the remote s3 path.
     *
     * @throws IOException if the tempFile fails to open a newInputStream
     */
    protected void sync() throws IOException {
        long size = Files.size(tempFile);
        String bucket = path.getFileStore().name();
        String key = path.getKey();

        File file = tempFile.toFile();
        PutObjectRequest request = new PutObjectRequest(bucket, key, file).withMetadata(new ObjectMetadata());
        if (lastModified != null)
        {
            request.withMetadata(S3BasicFileAttributeView.setMetadataTimes(new ObjectMetadata(), FileTime.fromMillis(lastModified.getTime()), null, FileTime.fromMillis(lastModified.getTime())));
        }

        AmazonS3 client = path.getFileSystem().getClient();
        // Uploads of > 5GB have to be done using a multipart upload instead of a single PUT, but we can get better
        // perf by allowing the parallelization of uploads for reasonably large files as well.
        if (size > 16 * 1024 * 1024)
        {
            TransferManager tm = TransferManagerBuilder.standard()
                    .withExecutorFactory(EXECUTOR_FACTORY)
                    .withS3Client(client)
                    .build();

            try {
                // TransferManager processes all transfers asynchronously,
                // so this call returns immediately.
                Upload upload = tm.upload(request);

                // Wait for the upload to finish before continuing.
                try {
                    upload.waitForCompletion();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
            finally
            {
                tm.shutdownNow(false);
            }
        }
        else
        {
            // Pass the raw file instead of an input stream, due to issue 36518
            // See https://github.com/aws/aws-sdk-java/issues/427

            // Stash the response from S3 to be used later
            putResult = client.putObject(request);
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return seekable.write(src);
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return seekable.truncate(size);
    }

    @Override
    public long size() throws IOException {
        return seekable.size();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return seekable.read(dst);
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return seekable.position(newPosition);
    }

    @Override
    public long position() throws IOException {
        return seekable.position();
    }

    public PutObjectResult getPutResult() {
        return putResult;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }
}