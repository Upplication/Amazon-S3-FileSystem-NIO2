package com.upplication.s3fs;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;
import com.upplication.s3fs.util.S3KeyHelper;

/**
 * S3 iterator over folders at first level.
 * Future verions of this class should be return the elements
 * in a incremental way when the #next() method is called.
 */
public class S3Iterator implements Iterator<Path> {

    private S3FileSystem s3FileSystem;
    private String bucket;
    private String key;

    private Iterator<S3Path> it;

    public S3Iterator(S3FileSystem s3FileSystem, String bucket, String key) {

        Preconditions.checkArgument(key != null && key.endsWith("/"), "key %s should be ended with slash '/'", key);

        this.bucket = bucket;
        // the only case i dont need the end slash is to list buckets content
        this.key = key.length() == 1 ? "" : key;
        this.s3FileSystem = s3FileSystem;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public S3Path next() {
        return getIterator().next();
    }

    @Override
    public boolean hasNext() {
        return getIterator().hasNext();
    }

    private Iterator<S3Path> getIterator() {
        if (it == null) {
            List<S3Path> listPath = new ArrayList<>();
            // TODO: need revision for better performance!
            // this request load objects that start with the key at all levels

            // iterator over this list
            ObjectListing current = s3FileSystem.getClient().listObjects(buildRequest());

            while (current.isTruncated()) {
                // parse the elements
                parseObjectListing(listPath, current);
                // continue
                current = s3FileSystem.getClient().listNextBatchOfObjects(current);
            }

            parseObjectListing(listPath, current);

            it = listPath.iterator();
        }

        return it;
    }

    private ListObjectsRequest buildRequest(){

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(key);
        request.setMarker(key);
        return request;
    }

    /**
     * add to the listPath the elements at the same level that s3Path
     * @param listPath List not null list to add
     * @param current ObjectListing to walk
     */
    private void parseObjectListing(List<S3Path> listPath, ObjectListing current) {
        for (final S3ObjectSummary objectSummary : current.getObjectSummaries()) {
            final String objectSummaryKey = objectSummary.getKey();
            // we only want the first level
            String key = getInmediateDescendent(this.key, objectSummaryKey);
            if (key != null){
            	S3Path descendentPart = new S3Path(s3FileSystem, "/" + objectSummary.getBucketName(), S3KeyHelper.getParts(key));

                if (!listPath.contains(descendentPart)){
                    listPath.add(descendentPart);
                }
            }
        }
    }

    /**
     * The current #buildRequest() get all subdirectories and her content.
     * This method filter the keyChild and check if is a inmediate
     * descendant of the keyParent parameter
     * @param keyParent String
     * @param keyChild String
     * @return String parsed
     *  or null when the keyChild and keyParent are the same and not have to be returned
     */
    private String getInmediateDescendent(String keyParent, String keyChild){

        keyParent = deleteExtraPath(keyParent);
        keyChild = deleteExtraPath(keyChild);

        final int parentLen = keyParent.length();
        final String childWithoutParent = deleteExtraPath(keyChild
                .substring(parentLen));

        String[] parts = childWithoutParent.split("/");

        if (parts.length > 0 && !parts[0].isEmpty()){
            return keyParent + "/" + parts[0];
        }
        else {
            return null;
        }

    }

    private String deleteExtraPath(String keyChild) {
        if (keyChild.startsWith("/")){
            keyChild = keyChild.substring(1);
        }
        if (keyChild.endsWith("/")){
            keyChild = keyChild.substring(0, keyChild.length() - 1);
        }
        return keyChild;
    }
}
