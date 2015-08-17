package com.upplication.s3fs.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.upplication.s3fs.S3FileAttributes;
import com.upplication.s3fs.S3Path;

import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

/**
 * Utilities to work with Amazon S3 Objects.
 */
public class S3Utils {

    /**
     * Get the {@link com.amazonaws.services.s3.model.S3ObjectSummary} that represent this Path or her first child if this path not exists
     * @param s3Path {@link com.upplication.s3fs.S3Path}
     * @return {@link com.amazonaws.services.s3.model.S3ObjectSummary}
     * @throws java.nio.file.NoSuchFileException if not found the path and any child
     */
    public S3ObjectSummary getS3ObjectSummary(S3Path s3Path) throws NoSuchFileException {
        String key = s3Path.getKey();
        String bucketName = s3Path.getFileStore().name();
        AmazonS3 client = s3Path.getFileSystem().getClient();
        try {
            ObjectMetadata metadata = client.getObjectMetadata(bucketName, key);
            S3ObjectSummary result = new S3ObjectSummary();
            result.setBucketName(bucketName);
            result.setETag(metadata.getETag());
            result.setKey(key);
            result.setLastModified(metadata.getLastModified());
            result.setSize(metadata.getContentLength());
            AccessControlList objectAcl = client.getObjectAcl(bucketName, key);
            result.setOwner(objectAcl.getOwner());
            return result;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 404)
                throw e;
        }

        try {
            // is a virtual directory
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucketName);
            request.setPrefix(key + "/");
            request.setMaxKeys(1);
            ObjectListing current = client.listObjects(request);
            if (!current.getObjectSummaries().isEmpty())
                return current.getObjectSummaries().get(0);
        } catch (Exception e) {
            //
        }
        throw new NoSuchFileException(bucketName + S3Path.PATH_SEPARATOR + key);
    }

    /**
     * getS3FileAttributes for the s3Path
     * @param s3Path S3Path mandatory not null
     * @return S3FileAttributes
     */
    public S3FileAttributes getS3FileAttributes(S3Path s3Path) throws NoSuchFileException {
        if (!s3Path.hasFileAttributes()) {
            s3Path.setFileAttributes(toS3FileAttributes(getS3ObjectSummary(s3Path), s3Path.getKey()));
        }
        return s3Path.getFileAttributes();
    }

    /**
     * convert S3ObjectSummary tpo S3FileAttributes
     * @param objectSummary S3ObjectSummary mandatory not null
     * @return S3FileAttributes
     */
    public static S3FileAttributes toS3FileAttributes(S3ObjectSummary objectSummary, String key) {
        // parse the data to BasicFileAttributes.
        FileTime lastModifiedTime = null;
        if (objectSummary.getLastModified() != null){
            lastModifiedTime = FileTime.from(objectSummary.getLastModified().getTime(), TimeUnit.MILLISECONDS);
        }
        long size = objectSummary.getSize();
        boolean directory = false;
        boolean regularFile = false;
        String resolvedKey = objectSummary.getKey();
        // check if is a directory and exists the key of this directory at amazon s3
        if (key.endsWith("/") && resolvedKey.equals(key) ||
                resolvedKey.equals(key + "/")) {
            directory = true;
        } else if (key.isEmpty()) { // is a bucket (no key)
            directory = true;
            resolvedKey = "/";
        }
        else if (!resolvedKey.equals(key) && resolvedKey.startsWith(key)) { // is a directory but not exists at amazon s3
            directory = true;
            // no metadata, we fake one
            size = 0;
            // delete extra part
            resolvedKey = key + "/";
        } else {
            regularFile = true;
        }
        return new S3FileAttributes(resolvedKey, lastModifiedTime, size, directory, regularFile);
    }
}
