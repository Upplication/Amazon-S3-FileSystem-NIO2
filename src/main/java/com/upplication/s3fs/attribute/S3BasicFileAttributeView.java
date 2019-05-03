package com.upplication.s3fs.attribute;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.upplication.s3fs.S3Path;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;


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
        // Overwrite the current metadata with the arguments
        if (lastModifiedTime != null) {
            metadataCopy.addUserMetadata(LABKEY_LAST_MODIFIED, Long.toString(lastModifiedTime.toMillis()));
        }
        if (lastAccessTime != null) {
            metadataCopy.addUserMetadata(LABKEY_LAST_ACCESS, Long.toString(lastAccessTime.toMillis()));
        }
        if (createTime != null) {
            metadataCopy.addUserMetadata(LABKEY_CREATE_TIME, Long.toString(createTime.toMillis()));
        }

        // S3 doesn't let you modify attributes of existing objects. However, you can set them as part of a copy
        // operation, and the copy's source and target locations can be identical. This means we don't have to download
        // and reupload the object, which is nice.
        CopyObjectRequest request = new CopyObjectRequest(bucketName, fileKey, bucketName, fileKey)
                .withSourceBucketName(bucketName)
                .withSourceKey(fileKey)
                .withNewObjectMetadata(metadataCopy);

        client.copyObject(request);
    }
}
