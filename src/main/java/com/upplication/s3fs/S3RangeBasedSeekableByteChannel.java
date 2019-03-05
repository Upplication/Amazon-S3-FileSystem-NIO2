package com.upplication.s3fs;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Optimized for seeking specific byte ranges for S3 objects. Does not support write and some other operations */
public class S3RangeBasedSeekableByteChannel implements SeekableByteChannel {

    private S3Path path;
    private boolean open = true;
    private long position = 0;

    /**
     * Open or creates a file, returning a seekable byte channel
     *
     * @param path    the path open or create
     */
    public S3RangeBasedSeekableByteChannel(S3Path path) {
        this.path = path;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        open = false;
    }

    /**
     * try to sync the temp file with the remote s3 path.
     *
     * @throws IOException if the tempFile fails to open a newInputStream
     */
    protected void sync() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int capacity = dst.capacity();

        String key = path.getKey();
        try (S3Object object = path.getFileSystem()
                .getClient()
                .getObject(new GetObjectRequest(path.getFileStore().name(), key).withRange(position, position + capacity))) {
            try (InputStream in = object.getObjectContent())
            {
                // Issue 36929 - be sure to fully consume all of the available bytes, not just do a single read()
                int index = 0;
                int bytesRead;
                byte[] array = dst.array();
                while ((bytesRead = in.read(array, index, array.length - index)) > 0)
                {
                    position += bytesRead;
                    index+= bytesRead;
                }
                return bytesRead;
            }
        }
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        position = newPosition;
        return this;
    }

    @Override
    public long position() throws IOException {
        return position;
    }
}