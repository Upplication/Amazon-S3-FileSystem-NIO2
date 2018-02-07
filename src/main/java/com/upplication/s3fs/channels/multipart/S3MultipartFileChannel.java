package com.upplication.s3fs.channels.multipart;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.github.davidmoten.guavamini.Sets;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.channels.S3Uploader;
import io.reactivex.Single;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.upplication.s3fs.AmazonS3Factory.MULTIPART_PART_SIZE;
import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.READ;

public class S3MultipartFileChannel extends FileChannel {

    private static final long DEFAULT_PART_SIZE = 32 * 1024 * 1024; // 32MB

    private final Set<? extends OpenOption> options;
    private final FileChannel backingFileChannel;
    private final Path backingFilePath;
    private final Subject<PartKey> partKeySubject = ReplaySubject.create();
    private final ObjectMetadata objectMetadata = new ObjectMetadata();
    private final Single<MultipartUploadSummary> multipartUploadSummary;
    private final S3Path path;

    private S3Object downloadObject;
    private ReadableByteChannel downloadChannel;

    public S3MultipartFileChannel(S3Path path, Set<? extends OpenOption> options, Properties properties) throws IOException {
        this.options = Collections.unmodifiableSet(new HashSet<>(options));
        this.path = path;
        String key = path.getKey();
        boolean exists = path.getFileSystem().provider().exists(path);

        if (exists && this.options.contains(StandardOpenOption.CREATE_NEW))
            throw new FileAlreadyExistsException(format("target already exists: %s", path));
        else if (!exists && !this.options.contains(StandardOpenOption.CREATE_NEW) &&
                !this.options.contains(StandardOpenOption.CREATE))
            throw new NoSuchFileException(format("target not exists: %s", path));

        backingFilePath = Files.createTempFile("temp-s3-", key.replaceAll("/", "_"));
        boolean removeTempFile = true;
        try {

            Set<? extends OpenOption> fileChannelOptions = new HashSet<>(this.options);
            fileChannelOptions.remove(StandardOpenOption.CREATE_NEW);
            backingFileChannel = FileChannel.open(backingFilePath, fileChannelOptions);

            removeTempFile = false;

            if (exists) {
                createDownloadChannel(path, key);
                multipartUploadSummary = null;
            } else {
                multipartUploadSummary = S3MultipartUploader.builder()
                        .path(path)
                        .s3Client(path.getFileSystem().getClient())
                        .objectMetadata(objectMetadata)
                        .changingParts(partKeySubject)
                        .uploadChannel(FileChannel.open(backingFilePath, Sets.newHashSet(READ)))
                        .partSize(Long.parseLong(properties.getProperty(MULTIPART_PART_SIZE, String.valueOf(DEFAULT_PART_SIZE))))
                        .build()
                        .upload(partKeySubject::onComplete);
            }
        } catch (Exception e) {
            partKeySubject.onError(e);
            throw new IllegalStateException(e);
        } finally {
            if (removeTempFile) {
                Files.deleteIfExists(backingFilePath);
            }
        }
    }

    private void createDownloadChannel(S3Path path, String key) {
        downloadObject = path.getFileSystem()
                .getClient()
                .getObject(path.getFileStore().getBucket().getName(), key);
        downloadChannel = Channels.newChannel(downloadObject.getObjectContent());
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return downloadChannel.read(dst);
    }

    @Override
    public int read(ByteBuffer dst, long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        final long startingPosition = position();
        int bytesWritten = backingFileChannel.write(src);

        partKeySubject.onNext(
                PartKey.builder()
                        .start(startingPosition)
                        .length(bytesWritten)
                        .build()
        );

        return bytesWritten;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        final long startingPosition = position();
        final long bytesWritten = backingFileChannel.write(srcs, offset, length);
        partKeySubject.onNext(
                PartKey.builder()
                        .start(startingPosition)
                        .length(bytesWritten)
                        .build()
        );
        return bytesWritten;
    }

    @Override
    public long position() throws IOException {
        return backingFileChannel.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        backingFileChannel.position(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return backingFileChannel.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        backingFileChannel.truncate(size);
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        backingFileChannel.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return backingFileChannel.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        long bytesWritten = backingFileChannel.transferFrom(src, position, count);
        partKeySubject.onNext(
                PartKey.builder()
                        .start(position)
                        .length(bytesWritten)
                        .build()
        );
        return bytesWritten;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        int bytesWritten = backingFileChannel.write(src, position);
        partKeySubject.onNext(
                PartKey.builder()
                        .start(position)
                        .length(bytesWritten)
                        .build()
        );
        return bytesWritten;
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        return backingFileChannel.map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return backingFileChannel.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return backingFileChannel.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (!this.options.contains(READ)) {
            completeUpload();
        }
        super.close();
        backingFileChannel.close();
        if (downloadObject != null) {
            downloadObject.close();
        }
        Files.deleteIfExists(backingFilePath);
    }

    @SneakyThrows
    private void completeUpload() {
        MultipartUploadSummary summary = multipartUploadSummary.blockingGet();
        if (!summary.isPerformed()) {
            InputStream in = Channels.newInputStream(FileChannel.open(backingFilePath, Sets.newHashSet(READ)));
            S3Uploader.builder()
                    .path(path)
                    .metadata(objectMetadata)
                    .in(in)
                    .size(summary.getBytesReceived())
                    .build()
                    .upload();
        }
    }

}
