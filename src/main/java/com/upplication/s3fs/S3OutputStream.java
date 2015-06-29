package com.upplication.s3fs;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;

/**
 * Writes data directly into an Amazon S3 object.
 */
public final class S3OutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(S3OutputStream.class);

    /**
     * Minimum part size of a part in a multipart upload: 5 MiB.
     *
     * @see  <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html">Amazon Simple Storage
     *       Service (S3) » API Reference » REST API » Operations on Objects » Upload Part</a>
     */
    private static final int MIN_UPLOAD_PART_SIZE = 5 << 20;

    /**
     * Maximum number of parts that may comprise a multipart upload: 10,000.
     *
     * @see  <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html">Amazon Simple Storage
     *       Service (S3) » API Reference » REST API » Operations on Objects » Upload Part</a>
     */
    private static final int MAX_ALLOWED_UPLOAD_PARTS = 10_000;

    /**
     * Amazon S3 API implementation to use.
     */
    private final AmazonS3 s3;

    /**
     * ID of the S3 object to store data into.
     */
    private final S3ObjectId objectId;

    /**
     * Amazon S3 storage class to apply to the newly created S3 object, if any.
     */
    private final StorageClass storageClass;

    /**
     * Metadata that will be attached to the stored S3 object.
     */
    private final ObjectMetadata metadata;

    /**
     * Indicates if the stream has been closed.
     */
    private volatile boolean closed;

    /**
     * Internal buffer. May be {@code null} if no bytes are buffered.
     */
    private byte[] buffer;

    /**
     * Number of bytes that are currently stored in the internal buffer. If {@code 0}, then {@code buffer} may also be
     * {@code null}.
     */
    private int bufferSize;

    /**
     * If a multipart upload is in progress, holds the ID for it, {@code null} otherwise.
     */
    private String uploadId;

    /**
     * If a multipart upload is in progress, holds the ETags of the uploaded parts, {@code null} otherwise.
     */
    private List<PartETag> partETags;

    /**
     * Creates a new {@code S3OutputStream} that writes data directly into the S3 object with the given {@code objectId}.
     * No special object metadata or storage class will be attached to the object.
     *
     * @param   s3        Amazon S3 API implementation to use
     * @param   objectId  ID of the S3 object to store data into
     *
     * @throws  NullPointerException  if at least one parameter is {@code null}
     */
    public S3OutputStream(final AmazonS3 s3, final S3ObjectId objectId) {
        this.s3 = requireNonNull(s3);
        this.objectId = requireNonNull(objectId);
        this.metadata = new ObjectMetadata();
        this.storageClass = null;
    }

    /**
     * Creates a new {@code S3OutputStream} that writes data directly into the S3 object with the given {@code objectId}.
     * No special object metadata will be attached to the object.
     *
     * @param   s3            Amazon S3 API implementation to use
     * @param   objectId      ID of the S3 object to store data into
     * @param   storageClass  Amazon S3 storage class to apply to the newly created S3 object, if any
     *
     * @throws  NullPointerException  if at least one parameter except {@code storageClass} is {@code null}
     */
    public S3OutputStream(final AmazonS3 s3, final S3ObjectId objectId, final StorageClass storageClass) {
        this.s3 = requireNonNull(s3);
        this.objectId = requireNonNull(objectId);
        this.storageClass = storageClass;
        this.metadata = new ObjectMetadata();
    }

    /**
     * Creates a new {@code S3OutputStream} that writes data directly into the S3 object with the given {@code objectId}.
     * The given {@code metadata} will be attached to the written object. No special storage class will be set for the
     * object.
     *
     * @param   s3        Amazon S3 API to use
     * @param   objectId  ID of the S3 object to store data into
     * @param   metadata  metadata to attach to the written object
     *
     * @throws  NullPointerException  if at least one parameter except {@code storageClass} is {@code null}
     */
    public S3OutputStream(final AmazonS3 s3, final S3ObjectId objectId, final ObjectMetadata metadata) {
        this.s3 = requireNonNull(s3);
        this.objectId = requireNonNull(objectId);
        this.storageClass = null;
        this.metadata = metadata.clone();
    }

    /**
     * Creates a new {@code S3OutputStream} that writes data directly into the S3 object with the given {@code objectId}.
     * The given {@code metadata} will be attached to the written object.
     *
     * @param   s3            Amazon S3 API to use
     * @param   objectId      ID of the S3 object to store data into
     * @param   storageClass  Amazon S3 storage class to apply to the newly created S3 object, if any
     * @param   metadata      metadata to attach to the written object
     *
     * @throws  NullPointerException  if at least one parameter except {@code storageClass} is {@code null}
     */
    public S3OutputStream(final AmazonS3 s3, final S3ObjectId objectId, final StorageClass storageClass,
            final ObjectMetadata metadata) {
        this.s3 = requireNonNull(s3);
        this.objectId = requireNonNull(objectId);
        this.storageClass = storageClass;
        this.metadata = metadata.clone();
    }

    @Override
    public void write(final int b) throws IOException {
        write(new byte[] {(byte) b});
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }

        if (len == 0) {
            return;
        }

        if (closed) {
            throw new IOException("Already closed");
        }

        synchronized (this) {
            if (uploadId != null && partETags.size() >= MAX_ALLOWED_UPLOAD_PARTS) {
                throw new IOException("Maximum number of upload parts reached");
            }

            if (len >= MIN_UPLOAD_PART_SIZE || bufferSize + len >= MIN_UPLOAD_PART_SIZE) {
                uploadPart((long) bufferSize + (long) len, bufferCombinedWith(b, off, len), false);
                bufferSize = 0;
            } else {
                if (buffer == null) {
                    buffer = new byte[MIN_UPLOAD_PART_SIZE];
                }

                System.arraycopy(b, off, buffer, bufferSize, len);
                bufferSize += len;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        synchronized (this) {
            if (uploadId == null) {
                putObject(bufferSize, bufferAsStream());
                buffer = null;
                bufferSize = 0;
            } else {
                uploadPart(bufferSize, bufferAsStream(), true);
                buffer = null;
                bufferSize = 0;
                completeMultipartUpload();
            }

            closed = true;
        }
    }

    private InitiateMultipartUploadResult initiateMultipartUpload() throws IOException {
        final InitiateMultipartUploadRequest request = //
            new InitiateMultipartUploadRequest(objectId.getBucket(), objectId.getKey(), metadata);

        if (storageClass != null) {
            request.setStorageClass(storageClass);
        }

        try {
            return s3.initiateMultipartUpload(request);
        } catch (final AmazonClientException e) {
            throw new IOException("Failed to initiate Amazon S3 multipart upload", e);
        }
    }

    private void uploadPart(final long contentLength, final InputStream content, final boolean lastPart)
        throws IOException {

        if (uploadId == null) {
            uploadId = initiateMultipartUpload().getUploadId();
            if (uploadId == null) {
                throw new IOException("Failed to get a valid multipart upload ID from Amazon S3");
            }

            partETags = new ArrayList<>();
        }

        final int partNumber = partETags.size() + 1;

        final UploadPartRequest request = new UploadPartRequest();
        request.setBucketName(objectId.getBucket());
        request.setKey(objectId.getKey());
        request.setUploadId(uploadId);
        request.setPartNumber(partNumber);
        request.setPartSize(contentLength);
        request.setInputStream(content);
        request.setLastPart(lastPart);

        LOG.debug("Uploading part {} with length {} for {} ", partNumber, contentLength, objectId);

        boolean success = false;
        try {
            final PartETag partETag = s3.uploadPart(request).getPartETag();
            LOG.debug("Uploaded part {} with length {} for {}: {}", //
                partETag.getPartNumber(), contentLength, objectId, partETag.getETag());
            partETags.add(partETag);

            success = true;
        } catch (final AmazonClientException e) {
            throw new IOException("Failed to upload multipart data to Amazon S3", e);
        } finally {
            if (!success) {
                closed = true;
                abortMultipartUpload();
            }
        }

        if (partNumber >= MAX_ALLOWED_UPLOAD_PARTS) {
            close();
        }
    }

    private void abortMultipartUpload() {
        LOG.debug("Aborting multipart upload {} for {}", uploadId, objectId);
        try {
            s3.abortMultipartUpload(new AbortMultipartUploadRequest( //
                    objectId.getBucket(), objectId.getKey(), uploadId));
            uploadId = null;
            partETags = null;
        } catch (final AmazonClientException e) {
            LOG.warn("Failed to abort multipart upload {}: {}", uploadId, e.getMessage());
        }
    }

    private void completeMultipartUpload() throws IOException {
        final int partCount = partETags.size();
        LOG.debug("Completing upload to {} consisting of {} parts", objectId, partCount);

        try {
            s3.completeMultipartUpload(new CompleteMultipartUploadRequest( //
                    objectId.getBucket(), objectId.getKey(), uploadId, partETags));
        } catch (final AmazonClientException e) {
            throw new IOException("Failed to complete Amazon S3 multipart upload", e);
        }

        LOG.debug("Completed upload to {} consisting of {} parts", objectId, partCount);

        uploadId = null;
        partETags = null;
    }

    private void putObject(final long contentLength, final InputStream content) throws IOException {

        final ObjectMetadata meta = metadata.clone();
        meta.setContentLength(contentLength);

        final PutObjectRequest request = new PutObjectRequest( //
                objectId.getBucket(), objectId.getKey(), content, meta);

        if (storageClass != null) {
            request.setStorageClass(storageClass);
        }

        try {
            s3.putObject(request);
        } catch (final AmazonClientException e) {
            throw new IOException("Failed to put data into Amazon S3 object", e);
        }
    }

    private InputStream bufferAsStream() {
        if (bufferSize > 0) {
            return new ByteArrayInputStream(buffer, 0, bufferSize);
        }

        return new InputStream() {
            @Override
            public int read() throws IOException {
                return -1;
            }
        };
    }

    private InputStream bufferCombinedWith(final byte[] b, final int off, final int len) {
        final ByteArrayInputStream stream = new ByteArrayInputStream(b, off, len);
        if (bufferSize < 1) {
            return stream;
        }

        return new SequenceInputStream(new ByteArrayInputStream(buffer, 0, bufferSize), stream);
    }
}
