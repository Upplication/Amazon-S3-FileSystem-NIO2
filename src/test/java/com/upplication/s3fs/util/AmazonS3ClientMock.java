package com.upplication.s3fs.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.*;
import java.util.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;
import com.amazonaws.util.StringUtils;
import org.apache.http.MethodNotSupportedException;

public class AmazonS3ClientMock extends AbstractAmazonS3 {
    /**
     * max elements amazon aws
     */
    private static final int LIMIT_AWS_MAX_ELEMENTS = 1000;

    // default owner
    private Owner defaultOwner = new Owner() {
        private static final long serialVersionUID = 5510838843790352879L;

        {
            setDisplayName("Mock");
            setId("1");
        }
    };

    private Path base;
    private Map<String, Owner> bucketOwners = new HashMap<>();

    public AmazonS3ClientMock(Path base) {
        this.base = base;
    }

    /**
     * list all objects without and return ObjectListing with all elements
     * and with truncated to false
     */
    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws AmazonClientException {
        String bucketName = listObjectsRequest.getBucketName();
        String prefix = listObjectsRequest.getPrefix();
        String marker = listObjectsRequest.getMarker();
        String delimiter = listObjectsRequest.getDelimiter();

        ObjectListing objectListing = new ObjectListing();
        objectListing.setBucketName(bucketName);
        objectListing.setPrefix(prefix);
        objectListing.setMarker(marker);
        objectListing.setDelimiter(delimiter);

        final Path bucket = find(bucketName);
        final TreeMap<String, S3Element> elems = new TreeMap<>();
        try {
            for (Path elem : Files.newDirectoryStream(bucket)) {
                S3Element element = parse(elem, bucket);
                if (!elems.containsKey(element.getS3Object().getKey()))
                    elems.put(element.getS3Object().getKey(), element);
            }
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
        Iterator<S3Element> iterator = elems.values().iterator();
        int i = 0;
        boolean waitForMarker = !StringUtils.isNullOrEmpty(marker);
        while (iterator.hasNext()) {
            S3Element elem = iterator.next();
            if (elem.getS3Object().getKey().equals("/"))
                continue;
            String key = elem.getS3Object().getKey();
            if (waitForMarker) {
                waitForMarker = !key.startsWith(marker);
                if (waitForMarker)
                    continue;
            }

            if (prefix != null && key.startsWith(prefix)) {
                int beginIndex = key.indexOf(prefix) + prefix.length();
                String rest = key.substring(beginIndex);
                if (delimiter != null && delimiter.length() > 0 && rest.contains(delimiter)) {
                    String substring = key.substring(0, beginIndex + rest.indexOf(delimiter));
                    if (!objectListing.getCommonPrefixes().contains(substring))
                        objectListing.getCommonPrefixes().add(substring);
                    continue;
                }
                S3ObjectSummary s3ObjectSummary = parseToS3ObjectSummary(elem);
                objectListing.getObjectSummaries().add(s3ObjectSummary);

                if (i + 1 == LIMIT_AWS_MAX_ELEMENTS && iterator.hasNext()) {
                    objectListing.setTruncated(true);
                    objectListing.setNextMarker(iterator.next().getS3Object().getKey());
                    return objectListing;
                }
                objectListing.setTruncated(false);

                i++;
            }

        }
        Collections.sort(objectListing.getObjectSummaries(), new Comparator<S3ObjectSummary>() {
            @Override
            public int compare(S3ObjectSummary o1, S3ObjectSummary o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        return objectListing;
    }

    @Override
    public ListObjectsV2Result listObjectsV2(String bucketName) throws AmazonClientException {
        return null;
    }

    @Override
    public ListObjectsV2Result listObjectsV2(String bucketName, String prefix) throws AmazonClientException {
        return null;
    }

    @Override
    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request) throws AmazonClientException {
        return null;
    }

    @Override
    public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) {
        ObjectListing objectListing = new ObjectListing();
        objectListing.setBucketName(previousObjectListing.getBucketName());
        objectListing.setPrefix(previousObjectListing.getPrefix());
        objectListing.setMarker(previousObjectListing.getMarker());
        objectListing.setDelimiter(previousObjectListing.getDelimiter());

        if (!previousObjectListing.isTruncated() || previousObjectListing.getNextMarker() == null) {
            return objectListing;
        }

        Path bucket = find(previousObjectListing.getBucketName());
        List<S3Element> elems = new ArrayList<>();
        try {
            for (Path elem : Files.newDirectoryStream(bucket)) {
                elems.add(parse(elem, bucket));
            }
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
        Collections.sort(elems, new Comparator<S3Element>() {
            @Override
            public int compare(S3Element o1, S3Element o2) {
                return o1.getS3Object().getKey().compareTo(o2.getS3Object().getKey());
            }
        });
        Iterator<S3Element> iterator = elems.iterator();

        int i = 0;
        boolean continueElement = false;

        while (iterator.hasNext()) {

            S3Element elem = iterator.next();

            if (!continueElement && elem.getS3Object().getKey().equals(previousObjectListing.getNextMarker())) {
                continueElement = true;
            }

            if (continueElement) {
                // TODO. add delimiter and marker support
                if (previousObjectListing.getPrefix() != null && elem.getS3Object().getKey().startsWith(previousObjectListing.getPrefix())) {

                    S3ObjectSummary s3ObjectSummary = parseToS3ObjectSummary(elem);
                    objectListing.getObjectSummaries().add(s3ObjectSummary);
                    // max 1000 elements at same time.
                    if (i + 1 == LIMIT_AWS_MAX_ELEMENTS && iterator.hasNext()) {
                        objectListing.setTruncated(true);
                        objectListing.setNextMarker(iterator.next().getS3Object().getKey());
                        return objectListing;
                    }
                    objectListing.setTruncated(false);
                    i++;
                }
            }
        }

        return objectListing;
    }

    @Override
    public ObjectListing listNextBatchOfObjects(ListNextBatchOfObjectsRequest listNextBatchOfObjectsRequest) throws AmazonClientException {
        return null;
    }

    /**
     * create a new S3ObjectSummary using the S3Element
     * @param elem S3Element to parse
     * @return S3ObjectSummary
     */
    private S3ObjectSummary parseToS3ObjectSummary(S3Element elem) {
        S3Object s3Object = elem.getS3Object();
        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setBucketName(s3Object.getBucketName());
        s3ObjectSummary.setKey(s3Object.getKey());
        s3ObjectSummary.setLastModified(objectMetadata.getLastModified());
        s3ObjectSummary.setOwner(getOwner(s3Object.getBucketName()));
        s3ObjectSummary.setETag(objectMetadata.getETag());
        s3ObjectSummary.setSize(objectMetadata.getContentLength());
        return s3ObjectSummary;
    }

    private Owner getOwner(String bucketName) {
        if (!bucketOwners.containsKey(bucketName))
            return defaultOwner;
        return bucketOwners.get(bucketName);
    }

    @Override
    public Owner getS3AccountOwner() throws AmazonClientException {
        return defaultOwner;
    }

    @Override
    public Owner getS3AccountOwner(GetS3AccountOwnerRequest getS3AccountOwnerRequest) throws AmazonClientException {
        return null;
    }

    public void setS3AccountOwner(Owner owner) {
        this.defaultOwner = owner;
    }

    @Override
    public List<Bucket> listBuckets() throws AmazonClientException {
        List<Bucket> result = new ArrayList<>();
        try {
            for (Path path : Files.newDirectoryStream(base)) {
                String bucketName = path.getFileName().toString();
                Bucket bucket = new Bucket(bucketName);
                bucket.setOwner(getOwner(bucketName));
                bucket.setCreationDate(new Date(Files.readAttributes(path, BasicFileAttributes.class).creationTime().toMillis()));
                result.add(bucket);
            }
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
        return result;
    }

    @Override
    public Bucket createBucket(CreateBucketRequest createBucketRequest) throws AmazonClientException {
        return createBucket(createBucketRequest.getBucketName());
    }

    @Override
    public Bucket createBucket(String bucketName) throws AmazonClientException {
        try {
            String name = bucketName.replaceAll("//", "");
            Path path = Files.createDirectories(base.resolve(name));
            Bucket bucket = new Bucket(name);
            bucket.setOwner(getOwner(name));
            bucket.setCreationDate(new Date(Files.readAttributes(path, BasicFileAttributes.class).creationTime().toMillis()));
            return bucket;
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key) throws AmazonClientException {
        Path elem = find(bucketName, key);
        if (elem != null) {
            try {
                return parse(elem, find(bucketName)).getPermission();
            } catch (IOException e) {
                throw new AmazonServiceException("Problem getting mock ACL: ", e);
            }
        }
        throw new AmazonServiceException("key not found, " + key);
    }

    @Override
    public AccessControlList getBucketAcl(String bucketName) throws AmazonClientException {
        Path bucket = find(bucketName);
        if (bucket == null) {
            throw new AmazonServiceException("bucket not found, " + bucketName);
        }
        return createAclPermission(bucket, bucketName);
    }

    @Override
    public S3Object getObject(String bucketName, String key) throws AmazonClientException {
        Path result = find(bucketName, key);
        if (result == null || !Files.exists(result)) {
            result = find(bucketName, key + "/");
        }
        if (result == null || !Files.exists(result)) {
            AmazonS3Exception amazonS3Exception = new AmazonS3Exception("not found with key: " + key);
            amazonS3Exception.setStatusCode(404);
            throw amazonS3Exception;
        }
        try {
            return parse(result, find(bucketName)).getS3Object();
        } catch (IOException e) {
            throw new AmazonServiceException("Problem getting Mock Object: ", e);
        }
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, File file) throws AmazonClientException {
        try {
            ByteArrayInputStream stream = new ByteArrayInputStream(Files.readAllBytes(file.toPath()));
            S3Element elem = parse(stream, bucketName, key);

            persist(bucketName, elem);

            PutObjectResult putObjectResult = new PutObjectResult();
            putObjectResult.setETag("3a5c8b1ad448bca04584ecb55b836264");
            return putObjectResult;
        } catch (IOException e) {
            throw new AmazonServiceException("", e);
        }

    }

    @Override
    public PutObjectResult putObject(String bucket, String keyName, InputStream inputStream, ObjectMetadata metadata) {
        S3Element elem = parse(inputStream, bucket, keyName);

        persist(bucket, elem);

        PutObjectResult putObjectResult = new PutObjectResult();
        putObjectResult.setETag("3a5c8b1ad448bca04584ecb55b836264");
        return putObjectResult;

    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, String content) throws AmazonClientException {
        return null;
    }

    /**
     * store in the memory map
     * @param bucketName bucket where persist
     * @param elem
     */
    private void persist(String bucketName, S3Element elem) {
        Path bucket = find(bucketName);
        String key = elem.getS3Object().getKey().replaceAll("/", "%2F");
        Path resolve = bucket.resolve(key);
        if (Files.exists(resolve))
            try {
                Files.delete(resolve);
            } catch (IOException e1) {
                // ignore
            }

        try {
            Files.createFile(resolve);
            S3ObjectInputStream objectContent = elem.getS3Object().getObjectContent();
            if (objectContent != null) {
                byte[] byteArray = IOUtils.toByteArray(objectContent);
                Files.write(resolve, byteArray);
            }
        } catch (IOException e) {
            throw new AmazonServiceException("Problem creating mock element: ", e);
        }
    }

    @Override
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws AmazonClientException {
        Path src = find(sourceBucketName, sourceKey);
        if (src != null && Files.exists(src)) {
            Path bucket = find(destinationBucketName);
            Path dest = bucket.resolve(destinationKey.replaceAll("/", "%2F"));
            try {
                Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new AmazonServiceException("Problem copying mock objects: ", e);
            }

            return new CopyObjectResult();
        }

        throw new AmazonServiceException("object source not found");
    }

    @Override
    public void deleteObject(String bucketName, String key) throws AmazonClientException {
        Path bucket = find(bucketName);
        Path resolve = bucket.resolve(key);
        if (Files.exists(resolve))
            try {
                Files.delete(resolve);
            } catch (IOException e) {
                throw new AmazonServiceException("Problem deleting mock object: ", e);
            }
        else {
            resolve = bucket.resolve(key.replaceAll("/", "%2F"));
            if (Files.exists(resolve))
                try {
                    Files.delete(resolve);
                } catch (IOException e) {
                    throw new AmazonServiceException("Problem deleting mock object: ", e);
                }
        }
    }

    private S3Element parse(InputStream stream, String bucketName, String key) {
        try (S3Object object = new S3Object()){
            object.setBucketName(bucketName);
            object.setKey(key);
            byte[] content = IOUtils.toByteArray(stream);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setLastModified(new Date());
            metadata.setContentLength(content.length);

            object.setObjectContent(new ByteArrayInputStream(content));
            object.setObjectMetadata(metadata);
            // TODO: create converter between path permission and s3 permission
            AccessControlList permission = createAllPermission(bucketName);
            return new S3Element(object, permission, false);
        } catch (IOException e) {
            throw new IllegalStateException("the stream is closed", e);
        }
    }

    private S3Element parse(Path elem, Path bucket) throws IOException {
        S3Object object = new S3Object();

        String bucketName = bucket.getFileName().toString();
        object.setBucketName(bucketName);

        String key = bucket.relativize(elem).toString().replaceAll("%2F", "/");
        boolean dir = key.endsWith("/") || key.isEmpty();
        object.setKey(key);

        ObjectMetadata metadata = new ObjectMetadata();
        BasicFileAttributes attr = Files.readAttributes(elem, BasicFileAttributes.class);
        metadata.setLastModified(new Date(attr.lastAccessTime().toMillis()));
        if (dir) {
            metadata.setContentLength(0);
            object.setObjectContent(null);
        } else {
            metadata.setContentLength(attr.size());
            object.setObjectContent(new ByteArrayInputStream(Files.readAllBytes(elem)));
        }

        object.setObjectMetadata(metadata);
        AccessControlList permission = createAclPermission(elem, bucketName);

        return new S3Element(object, permission, dir);
    }

    /**
     * create the com.amazonaws.services.s3.model.AccessControlList from a Path
     *
     * @param elem Path
     * @param bucketName String
     * @return AccessControlList never null
     */
    private AccessControlList createAclPermission(Path elem, String bucketName) {
        AccessControlList res = new AccessControlList();
        final Owner owner = getOwner(bucketName);
        res.setOwner(owner);
        Grantee grant = new Grantee() {
            @Override
            public void setIdentifier(String id) {
                //
            }

            @Override
            public String getTypeIdentifier() {
                return owner.getId();
            }

            @Override
            public String getIdentifier() {
                return owner.getId();
            }
        };

        try {
            Set<PosixFilePermission> permission = Files.readAttributes(elem, PosixFileAttributes.class).permissions();
            for (PosixFilePermission posixFilePermission : permission) {
                switch (posixFilePermission) {
                    case GROUP_READ:
                    case OTHERS_READ:
                    case OWNER_READ:
                        res.grantPermission(grant, Permission.Read);
                        break;
                    case OWNER_WRITE:
                    case GROUP_WRITE:
                    case OTHERS_WRITE:
                        res.grantPermission(grant, Permission.Write);
                        break;
                    case OWNER_EXECUTE:
                    case GROUP_EXECUTE:
                    case OTHERS_EXECUTE:
                        res.grantPermission(grant, Permission.WriteAcp);
                        res.grantPermission(grant, Permission.ReadAcp);
                        break;

                }
            }
        }catch (IOException e) {
            throw new RuntimeException(e);
        }

        return res;
    }

    private AccessControlList createAllPermission(String bucketName) {
        AccessControlList res = new AccessControlList();
        final Owner owner = getOwner(bucketName);
        res.setOwner(owner);
        Grantee grant = new Grantee() {
            @Override
            public void setIdentifier(String id) {
                //
            }

            @Override
            public String getTypeIdentifier() {
                return owner.getId();
            }

            @Override
            public String getIdentifier() {
                return owner.getId();
            }
        };

        res.grantPermission(grant, Permission.FullControl);
        res.grantPermission(grant, Permission.Read);
        res.grantPermission(grant, Permission.Write);
        return res;
    }

    public AccessControlList createReadOnly(String bucketName) {
        return createReadOnly(getOwner(bucketName));
    }

    public AccessControlList createReadOnly(final Owner owner) {
        AccessControlList res = new AccessControlList();
        res.setOwner(owner);
        Grantee grant = new Grantee() {
            @Override
            public void setIdentifier(String id) {
                //
            }

            @Override
            public String getTypeIdentifier() {
                return owner.getId();
            }

            @Override
            public String getIdentifier() {
                return owner.getId();
            }
        };
        res.grantPermission(grant, Permission.Read);
        return res;
    }

    private Path find(String bucketName, final String key) {
        final Path bucket = find(bucketName);
        if (bucket == null || !Files.exists(bucket)) {
            return null;
        }
        try {
            final String fileKey = key.replaceAll("/", "%2F");
            final List<Path> matches = new ArrayList<Path>();
            Files.walkFileTree(bucket, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    String relativize = bucket.relativize(dir).toString();
                    if (relativize.equals(fileKey)) {
                        matches.add(dir);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String relativize = bucket.relativize(file).toString();
                    if (relativize.equals(fileKey)) {
                        matches.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            if (!matches.isEmpty())
                return matches.iterator().next();
        } catch (IOException e) {
            throw new AmazonServiceException("Problem getting mock S3Element: ", e);
        }

        return null;
    }

    private Path find(String bucketName) {
        return base.resolve(bucketName);
    }

    public static class S3Element {

        private S3Object s3Object;
        private boolean directory;
        private AccessControlList permission;

        public S3Element(S3Object s3Object, AccessControlList permission, boolean directory) {
            this.s3Object = s3Object;
            this.directory = directory;
            this.permission = permission;
        }

        public S3Object getS3Object() {
            return s3Object;
        }

        public void setS3Object(S3Object s3Object) {
            this.s3Object = s3Object;
        }

        public AccessControlList getPermission() {
            return permission;
        }

        public boolean isDirectory() {
            return directory;
        }

        public void setDirectory(boolean directory) {
            this.directory = directory;
        }

        public void setPermission(AccessControlList permission) {
            this.permission = permission;
        }

        @Override
        public boolean equals(Object object) {

            if (object == null) {
                return false;
            }

            if (object instanceof S3Element) {
                S3Element elem = (S3Element) object;
                // only is the same if bucketname and key are not null and are the same
                if (elem.getS3Object() != null && this.getS3Object() != null && elem.getS3Object().getBucketName() != null
                        && elem.getS3Object().getBucketName().equals(this.getS3Object().getBucketName()) && elem.getS3Object().getKey() != null
                        && elem.getS3Object().getKey().equals(this.getS3Object().getKey())) {
                    return true;
                }

                return false;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = s3Object != null && s3Object.getBucketName() != null ? s3Object.getBucketName().hashCode() : 0;
            result = 31 * result + (s3Object != null && s3Object.getKey() != null ? s3Object.getKey().hashCode() : 0);
            return result;
        }
    }

    @Override
    public ObjectMetadata getObjectMetadata(String bucketName, String key) {
        S3Object object = getObject(bucketName, key);
        if (object.getKey().equals(key))
            return object.getObjectMetadata();
        AmazonS3Exception exception = new AmazonS3Exception("Resource not available: " + bucketName + "/" + key);
        exception.setStatusCode(404);
        throw exception;
    }

    @Override
    public boolean doesBucketExist(String bucketName) throws AmazonClientException {
        return Files.exists(base.resolve(bucketName));
    }

    @Override
    public HeadBucketResult headBucket(HeadBucketRequest headBucketRequest) throws AmazonClientException {
        return null;
    }

    public MockBucket bucket(String bucketName) throws IOException {
        return new MockBucket(this, Files.createDirectories(base.resolve(bucketName)));
    }

    public Path bucket(String bucketName, Owner owner) throws IOException {
        bucketOwners.put(bucketName, owner);
        return Files.createDirectories(base.resolve(bucketName));
    }

    @Override
    public void deleteBucket(String bucketName) throws AmazonClientException {
        try {
            Path bucket = base.resolve(bucketName);
            Files.walkFileTree(bucket, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
    }

    public void addFile(Path bucket, String fileName) throws IOException {
        if (fileName.endsWith("/"))
            fileName.substring(0, fileName.length() - 1);
        Files.createFile(bucket.resolve(fileName.replaceAll("/", "%2F")));
    }

    public void addFile(Path bucket, String fileName, byte[] content) throws IOException {
        addFile(bucket, fileName, content, new FileAttribute<?>[0]);
    }

    public void addFile(Path bucket, String fileName, byte[] content, FileAttribute<?>... attrs) throws IOException {
        if (fileName.endsWith("/"))
            fileName.substring(0, fileName.length() - 1);
        Path file = Files.createFile(bucket.resolve(fileName.replaceAll("/", "%2F")), attrs);
        try (OutputStream outputStream = Files.newOutputStream(file)) {
            outputStream.write(content);
        }
    }

    public void addDirectory(Path bucket, String directoryName) throws IOException {
        if (!directoryName.endsWith("/"))
            directoryName += "/";
        Files.createFile(bucket.resolve(directoryName.replaceAll("/", "%2F")));
    }

    public void clear() {
        try {
            Files.walkFileTree(base, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (dir != base)
                        Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setEndpoint(String endpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRegion(Region region) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setS3ClientOptions(S3ClientOptions clientOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectListing listObjects(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectListing listObjects(String bucketName, String prefix) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public VersionListing listVersions(String bucketName, String prefix) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public VersionListing listNextBatchOfVersions(ListNextBatchOfVersionsRequest listNextBatchOfVersionsRequest) throws AmazonClientException {
        return null;
    }

    @Override
    public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker, String delimiter, Integer maxResults)
            throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public VersionListing listVersions(ListVersionsRequest listVersionsRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getBucketLocation(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bucket createBucket(String bucketName, com.amazonaws.services.s3.model.Region region) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bucket createBucket(String bucketName, String region) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key, String versionId) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AccessControlList getObjectAcl(GetObjectAclRequest getObjectAclRequest) throws AmazonClientException {
        return null;
    }

    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setObjectAcl(SetObjectAclRequest setObjectAclRequest) throws AmazonClientException {

    }

    @Override
    public void setBucketAcl(SetBucketAclRequest setBucketAclRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketAcl(String bucketName, AccessControlList acl) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketAcl(String bucketName, CannedAccessControlList acl) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public S3Object getObject(GetObjectRequest getObjectRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getObjectAsString(String bucketName, String key) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucket(DeleteBucketRequest deleteBucketRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest) throws AmazonClientException {
        if (putObjectRequest.getBucketName() != null && putObjectRequest.getKey() != null && putObjectRequest.getFile() != null) {
            return putObject(putObjectRequest.getBucketName(), putObjectRequest.getKey(), putObjectRequest.getFile());
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest copyPartRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteObject(DeleteObjectRequest deleteObjectRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteVersion(String bucketName, String key, String versionId) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteVersion(DeleteVersionRequest deleteVersionRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketLoggingConfiguration getBucketLoggingConfiguration(GetBucketLoggingConfigurationRequest getBucketLoggingConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketVersioningConfiguration getBucketVersioningConfiguration(GetBucketVersioningConfigurationRequest getBucketVersioningConfigurationRequest) throws AmazonClientException {
        return null;
    }

    @Override
    public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest) throws AmazonClientException{
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketLifecycleConfiguration(String bucketName, BucketLifecycleConfiguration bucketLifecycleConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketLifecycleConfiguration(String bucketName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(GetBucketCrossOriginConfigurationRequest getBucketCrossOriginConfigurationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketCrossOriginConfiguration(String bucketName, BucketCrossOriginConfiguration bucketCrossOriginConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketCrossOriginConfiguration(String bucketName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketTaggingConfiguration getBucketTaggingConfiguration(GetBucketTaggingConfigurationRequest getBucketTaggingConfigurationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketTaggingConfiguration(String bucketName, BucketTaggingConfiguration bucketTaggingConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketTaggingConfiguration(String bucketName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketNotificationConfiguration getBucketNotificationConfiguration(GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest) throws AmazonClientException{
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketNotificationConfiguration(String bucketName, BucketNotificationConfiguration bucketNotificationConfiguration) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketWebsiteConfiguration(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketPolicy getBucketPolicy(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketPolicy(String bucketName, String policyText) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketPolicy(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartListing listParts(ListPartsRequest request) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreObject(RestoreObjectRequest request) throws AmazonServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreObject(String bucketName, String key, int expirationInDays) throws AmazonServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableRequesterPays(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void disableRequesterPays(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequesterPaysEnabled(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketReplicationConfiguration(String bucketName, BucketReplicationConfiguration configuration) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketReplicationConfiguration(SetBucketReplicationConfigurationRequest setBucketReplicationConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketReplicationConfiguration getBucketReplicationConfiguration(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketReplicationConfiguration getBucketReplicationConfiguration(GetBucketReplicationConfigurationRequest getBucketReplicationConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketReplicationConfiguration(String bucketName) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBucketReplicationConfiguration(DeleteBucketReplicationConfigurationRequest request) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean doesObjectExist(String bucketName, String objectName) throws AmazonClientException {
        // TODO: review
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketAccelerateConfiguration getBucketAccelerateConfiguration(String bucket) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketAccelerateConfiguration getBucketAccelerateConfiguration(GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketAccelerateConfiguration(String bucketName, BucketAccelerateConfiguration accelerateConfiguration) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBucketAccelerateConfiguration(SetBucketAccelerateConfigurationRequest setBucketAccelerateConfigurationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public com.amazonaws.services.s3.model.Region getRegion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL getUrl(String bucketName, String key) {
        // TODO: review
        throw new UnsupportedOperationException();
    }

    @Override
    public AmazonS3Waiters waiters() {
        throw new UnsupportedOperationException();
    }
}