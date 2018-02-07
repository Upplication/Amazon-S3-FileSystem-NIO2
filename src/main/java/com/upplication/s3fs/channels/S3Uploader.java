package com.upplication.s3fs.channels;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.upplication.s3fs.S3Path;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.tika.Tika;

import java.io.BufferedInputStream;
import java.io.InputStream;

@Builder
public class S3Uploader {

    private final S3Path path;
    private final InputStream in;
    private final long size;
    private final ObjectMetadata metadata;

    @SneakyThrows
    public void upload() {
        try (InputStream stream = new BufferedInputStream(in)) {
            metadata.setContentLength(size);
            metadata.setContentType(new Tika().detect(stream, path.getFileName().toString()));
            String bucket = path.getFileStore().name();
            String key = path.getKey();
            path.getFileSystem().getClient().putObject(bucket, key, stream, metadata);
        }
    }

}
