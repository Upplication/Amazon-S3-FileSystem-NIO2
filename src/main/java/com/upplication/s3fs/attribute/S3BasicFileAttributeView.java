package com.upplication.s3fs.attribute;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.upplication.s3fs.S3Path;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class S3BasicFileAttributeView implements BasicFileAttributeView {

    /**
     * S3 doesn't let you set an object's modified time, to match the source file, for example. Thus, we
     * use custom attributes to stash what we want to treat at the "real" last modified and other timestamps.
     */
    public static final String LABKEY_LAST_MODIFIED = "labkey-last-modified";
    public static final String LABKEY_LAST_ACCESS = "labkey-last-access";
    public static final String LABKEY_CREATE_TIME = "labkey-create-time";

    private S3Path s3Path;

    public S3BasicFileAttributeView(S3Path s3Path) {
        this.s3Path = s3Path;
    }

    @Override
    public String name() {
        return "basic";
    }

    @Override
    public BasicFileAttributes readAttributes() throws IOException {
        return s3Path.getFileSystem().provider().readAttributes(s3Path, BasicFileAttributes.class);
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        AmazonS3 client = s3Path.getFileStore().getFileSystem().getClient();
        String bucketName = s3Path.getFileStore().getBucket().getName();
        String fileKey = s3Path.getKey();
        ObjectMetadata metadataCopy = client.getObjectMetadata(bucketName, fileKey).clone();
        setMetadataTimes(metadataCopy, lastModifiedTime, lastAccessTime, createTime);

        // S3 doesn't let you modify attributes of existing objects. However, you can set them as part of a copy
        // operation, and the copy's source and target locations can be identical. This means we don't have to download
        // and reupload the object, which is nice.

        long objectSize = metadataCopy.getContentLength();
        // S3 doesn't support single-copy requests when the object is >5GB, so use a multi-part approach when needed
        // Taken from https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsUsingLLJavaMPUapi.html

        long partSize = 5L * 1024L * 1024L * 1024L - 1; // 5GB chunks
        if (objectSize > partSize) {

            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, fileKey).withObjectMetadata(metadataCopy);
            InitiateMultipartUploadResult initResult = client.initiateMultipartUpload(initRequest);

            long bytePosition = 0;
            int partNum = 1;
            List<CopyPartResult> copyResponses = new ArrayList<>();
            while (bytePosition < objectSize) {
                // The last part might be smaller than partSize, so check to make sure
                // that lastByte isn't beyond the end of the object.
                long lastByte = Math.min(bytePosition + partSize - 1, objectSize - 1);

                // Copy this part.
                CopyPartRequest copyRequest = new CopyPartRequest()
                        .withSourceBucketName(bucketName)
                        .withSourceKey(fileKey)
                        .withDestinationBucketName(bucketName)
                        .withDestinationKey(fileKey)
                        .withUploadId(initResult.getUploadId())
                        .withFirstByte(bytePosition)
                        .withLastByte(lastByte)
                        .withPartNumber(partNum++);
                copyResponses.add(client.copyPart(copyRequest));
                bytePosition += partSize;
            }

            // Complete the upload request to concatenate all uploaded parts and make the copied object available.
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                    bucketName,
                    fileKey,
                    initResult.getUploadId(),
                    getETags(copyResponses));
            client.completeMultipartUpload(completeRequest);
        }
        else {
            CopyObjectRequest request = new CopyObjectRequest(bucketName, fileKey, bucketName, fileKey)
                    .withSourceBucketName(bucketName)
                    .withSourceKey(fileKey)
                    .withNewObjectMetadata(metadataCopy);

            client.copyObject(request);
        }
    }

    public static ObjectMetadata setMetadataTimes(ObjectMetadata metadata, FileTime lastModified, FileTime lastAccess, FileTime createTime)
    {
        // Overwrite the current metadata with the arguments
        if (lastModified != null) {
            metadata.addUserMetadata(LABKEY_LAST_MODIFIED, Long.toString(lastModified.toMillis()));
        }
        if (lastAccess != null) {
            metadata.addUserMetadata(LABKEY_LAST_ACCESS, Long.toString(lastAccess.toMillis()));
        }
        if (createTime != null) {
            metadata.addUserMetadata(LABKEY_CREATE_TIME, Long.toString(createTime.toMillis()));
        }
        return metadata;
    }

    // This is a helper function to construct a list of ETags.
    private List<PartETag> getETags(List<CopyPartResult> responses) {
        List<PartETag> etags = new ArrayList<>();
        for (CopyPartResult response : responses) {
            etags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return etags;
    }
}
