package com.upplication.s3fs;

import com.amazonaws.services.s3.model.PutObjectResult;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

/** Special subclass that gives access to the result object from the PUT call to S3 */
public class S3FSOutputStream extends FilterOutputStream {

    private final S3SeekableByteChannel channel;

    public S3FSOutputStream(OutputStream out, S3SeekableByteChannel channel) {
        super(out);
        this.channel = channel;
    }

    public PutObjectResult getPutResult() {
        return channel.getPutResult();
    }

    public void setLastModified(Date lastModified) {
        this.channel.setLastModified(lastModified);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // Override as FilterOutputStream implements as calls to the single-byte write() method
        out.write(b, off, len);
    }
}
