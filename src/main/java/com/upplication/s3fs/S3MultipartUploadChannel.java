package com.upplication.s3fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class S3MultipartUploadChannel extends FileChannel {
    private final WritableByteChannel writableChannel;

    private final S3MultipartUploadOutputStream outputStream;

    private long size;

    public S3MultipartUploadChannel(final S3MultipartUploadOutputStream outputStream) {
        this.outputStream = outputStream;
        this.writableChannel = Channels.newChannel(outputStream);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        throw new NonReadableChannelException();
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        throw new NonReadableChannelException();
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        throw new NonReadableChannelException();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        synchronized (writableChannel) {
            final int written = this.writableChannel.write(src);

            size += written;

            return written;
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long position() throws IOException {
        return size;
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        return this;
    }

    @Override
    public long size() throws IOException {
        return size;
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void implCloseChannel() throws IOException {
        outputStream.close();
    }
}
