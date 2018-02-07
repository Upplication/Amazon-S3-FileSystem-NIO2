package com.upplication.s3fs.channels.multipart;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.upplication.s3fs.S3Path;
import io.reactivex.Observable;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.tika.Tika;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.nio.channels.Channels.newInputStream;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public class S3MultipartUploader extends MultipartUploader<UploadPartResult> {

    private final ObjectMetadata objectMetadata;
    private final AmazonS3 s3Client;
    private final S3Path path;
    private final FileChannel uploadChannel;

    private String uploadId;

    @Builder
    public S3MultipartUploader(
            Observable<PartKey> changingParts,
            ObjectMetadata objectMetadata,
            AmazonS3 s3Client,
            S3Path path,
            FileChannel uploadChannel,
            Long partSize) {
        super(changingParts, partSize);
        this.objectMetadata = objectMetadata;
        this.s3Client = s3Client;
        this.path = path;
        this.uploadChannel = uploadChannel;
    }

    @Override
    protected void startTransfer(List<PartKey> object) {
        final String bucket = path.getFileStore().name();
        final String key = path.getKey();
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key);
        try {
            objectMetadata.setContentType(new Tika().detect(newInputStream(uploadChannel), path.getFileName().toString()));
        } catch (IOException e) {
            throw new IllegalStateException("Could not determine content type", e);
        }
        initRequest.setObjectMetadata(objectMetadata);
        uploadId = s3Client.initiateMultipartUpload(initRequest).getUploadId();
    }

    @Override
    protected Part<UploadPartResult> uploadNewPart(int partNo, PartKey partKey) {
        UploadPartResult uploadPartResult = s3Client.uploadPart(anUploadPartRequest(partNo, partKey));
        return new Part<>(partKey, partNo, uploadPartResult);
    }

    @Override
    protected String endTransfer() {
        final List<PartETag> partEtags = getManagedParts().values().stream()
                .map(Part::getValue)
                .map(UploadPartResult::getPartETag)
                .collect(Collectors.toList());

        final String bucket = path.getFileStore().name();
        final String key = path.getKey();
        return s3Client.completeMultipartUpload(
                new CompleteMultipartUploadRequest(
                        bucket,
                        key,
                        uploadId,
                        partEtags
                )
        ).getETag();
    }

    @SneakyThrows
    private UploadPartRequest anUploadPartRequest(int partId, PartKey partKey) {
        final String bucket = path.getFileStore().name();
        final String key = path.getKey();
        MappedByteBuffer bytesToWrite = uploadChannel.map(READ_ONLY, partKey.getStart(), partKey.getLength());
        return new UploadPartRequest()
                .withUploadId(uploadId)
                .withPartNumber(partId)
                .withPartSize(partKey.getLength())
                .withInputStream(new ByteBufferInputStream(bytesToWrite))
                .withBucketName(bucket)
                .withKey(key);
    }

}
