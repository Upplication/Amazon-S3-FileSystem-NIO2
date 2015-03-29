package com.upplication.s3fs.util;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.upplication.s3fs.AmazonS3Client;
import com.upplication.s3fs.S3Path;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

public class S3ObjectSummaryLookup {

    /**
     * Get the {@link com.amazonaws.services.s3.model.S3ObjectSummary} that represent this Path or her first child if this path not exists
     * @param s3Path {@link com.upplication.s3fs.S3Path}
     * @return {@link com.amazonaws.services.s3.model.S3ObjectSummary}
     * @throws java.nio.file.NoSuchFileException if not found the path and any child
     */
    public S3ObjectSummary lookup(S3Path s3Path) throws NoSuchFileException {

        AmazonS3Client client = s3Path.getFileSystem().getClient();

        ObjectMetadata objectMetadata = getS3ObjectMetadata(s3Path);

        if (objectMetadata != null) {
            S3ObjectSummary result = new S3ObjectSummary();
            result.setBucketName(s3Path.getBucket());
            result.setETag(objectMetadata.getETag());
            result.setKey(s3Path.getKey());
            result.setLastModified(objectMetadata.getLastModified());
            result.setSize(objectMetadata.getContentLength());

            return result;
        }

        // is a virtual directory
        String key = s3Path.getKey() + "/";

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(s3Path.getBucket());
        request.setPrefix(key);
        request.setMaxKeys(1);
        ObjectListing current = client.listObjects(request);

        if (!current.getObjectSummaries().isEmpty()){
            return current.getObjectSummaries().get(0);
        }
        else{
            throw new NoSuchFileException(s3Path.toString());
        }
    }

    public ObjectMetadata getS3ObjectMetadata(S3Path s3Path) {
        AmazonS3Client client = s3Path.getFileSystem().getClient();
        try {
            return client.getObjectMetadata(s3Path.getBucket(), s3Path.getKey());
        }
        catch (AmazonS3Exception e){
            if (e.getStatusCode() != 404){
                throw e;
            }
            return null;
        }
    }

    /**
     * get S3Object represented by this S3Path try to access with or without end slash '/'
     * @param s3Path S3Path
     * @return S3Object or null if not exists
     */
    @Deprecated
    private S3Object getS3Object(S3Path s3Path){

        AmazonS3Client client = s3Path.getFileSystem()
                .getClient();

        S3Object object = getS3Object(s3Path.getBucket(), s3Path.getKey(), client);

        if (object != null) {
            return object;
        }
        else{
            return getS3Object(s3Path.getBucket(), s3Path.getKey() + "/", client);
        }
    }

    /**
     * get s3Object with S3Object#getObjectContent closed
     * @param bucket String bucket
     * @param key String key
     * @param client AmazonS3Client client
     * @return S3Object
     */
    @Deprecated
    private S3Object getS3Object(String bucket, String key, AmazonS3Client client){
        try {
            S3Object object = client
                    .getObject(bucket, key);
            // FIXME: how only get the metadata
            if (object.getObjectContent() != null){
                object.getObjectContent().close();
            }
            return object;
        }
        catch (AmazonS3Exception e){
            if (e.getStatusCode() != 404){
                throw e;
            }
            return null;
        }
        catch (IOException e){
            throw new RuntimeException(e);
        }
    }
}
