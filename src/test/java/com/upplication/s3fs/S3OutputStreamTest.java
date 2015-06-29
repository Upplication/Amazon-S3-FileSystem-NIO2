package com.upplication.s3fs;

import static org.hamcrest.CoreMatchers.is;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.mockito.Matchers.any;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;

import org.mockito.invocation.InvocationOnMock;

import org.mockito.runners.MockitoJUnitRunner;

import org.mockito.stubbing.Answer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import com.google.common.io.ByteStreams;

@RunWith(MockitoJUnitRunner.class)
public class S3OutputStreamTest {

    @Mock
    private AmazonS3 s3;

    @Captor
    private ArgumentCaptor<PutObjectRequest> putObjectCaptor;

    @Captor
    private ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadCaptor;

    private final S3ObjectId objectId = new S3ObjectId("test", "object");

    private String uploadId;
    private boolean lastUploadPartSeen;
    private byte[] uploadedParts;

    private S3OutputStream underTest;

    @Before
    public void initializeTest() {
        when(s3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenAnswer(new InitiateMultipartUploadHandler());
        when(s3.uploadPart(any(UploadPartRequest.class))).thenAnswer(new UploadPartHandler());
        when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenAnswer(new CompleteMultipartUploadHandler());
        underTest = new S3OutputStream(s3, objectId);
    }

    @Test
    public void openAndCloseProducesEmptyObject() throws IOException {
        underTest.close();
        assertThatBytesHaveBeenPut(new byte[0]);
    }

    @Test
    public void zeroBytesWrittenProduceEmptyObject() throws IOException {
        underTest.write(new byte[0]);
        underTest.close();
        assertThatBytesHaveBeenPut(new byte[0]);
    }

    @Test
    public void smallDataUsesPutObject() throws IOException {
        final byte[] data = newRandomData(64);

        underTest.write(data);
        underTest.close();
        assertThatBytesHaveBeenPut(data);
    }

    @Test
    public void bigDataDataUsesMultipartUpload() throws IOException {
        final int sixMiB = 6 * 1024 * 1024;
        final int threeMiB = 3 * 1024 * 1024;

        final byte[] data = newRandomData(sixMiB);

        underTest.write(data, 0, threeMiB);
        underTest.write(data, threeMiB, threeMiB);
        underTest.close();
        assertThatBytesHaveBeenUploaded(data);
        assertThat("Multipart upload didn't have a last part", lastUploadPartSeen);
    }

    private void assertThatBytesHaveBeenPut(final byte[] data) throws IOException {
        verify(s3).putObject(putObjectCaptor.capture());
        verifyNoMoreInteractions(s3);

        final PutObjectRequest putObjectRequest = putObjectCaptor.getValue();
        assertThat(putObjectRequest.getMetadata().getContentLength(), is((long) data.length));

        final byte[] putData;
        try(final InputStream stream = putObjectCaptor.getValue().getInputStream()) {
            putData = new byte[data.length];
            ByteStreams.readFully(stream, putData);
            assertThat(stream.read(), is(-1));
        }

        assertThat("Mismatch between expected content and actual content", Arrays.equals(data, putData));
    }

    private void assertThatBytesHaveBeenUploaded(final byte[] data) {
        final InOrder inOrder = inOrder(s3);

        inOrder.verify(s3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        inOrder.verify(s3, atLeastOnce()).uploadPart(any(UploadPartRequest.class));
        inOrder.verify(s3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        inOrder.verifyNoMoreInteractions();

        assertThat("Mismatch between expected content and actual content", Arrays.equals(data, uploadedParts));
    }

    private int resizeUploadedPartsBy(final int contentLength) {
        if (uploadedParts == null) {
            uploadedParts = new byte[contentLength];
            return 0;
        }

        final int offset = uploadedParts.length;
        uploadedParts = Arrays.copyOf(uploadedParts, offset + contentLength);
        return offset;
    }

    private static byte[] newRandomData(final int size) {
        final byte[] data = new byte[size];
        new Random().nextBytes(data);
        return data;
    }

    private final class InitiateMultipartUploadHandler implements Answer<InitiateMultipartUploadResult> {
        @Override
        public InitiateMultipartUploadResult answer(final InvocationOnMock invocation) throws Throwable {
            uploadId = UUID.randomUUID().toString();
            lastUploadPartSeen = false;
            uploadedParts = null;

            final InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
            result.setUploadId(uploadId);
            return result;
        }
    }

    private final class UploadPartHandler implements Answer<UploadPartResult> {
        @Override
        public UploadPartResult answer(final InvocationOnMock invocation) throws IOException {
            final UploadPartRequest request = (UploadPartRequest) invocation.getArguments()[0];

            if (uploadId != null && uploadId.equals(request.getUploadId())) {
                assertThat("Already processed last part of this multipart upload", !lastUploadPartSeen);
                lastUploadPartSeen = request.isLastPart();

                final int partSize = (int) request.getPartSize();
                final int offset = resizeUploadedPartsBy(partSize);
                try(final InputStream in = request.getInputStream()) {
                    ByteStreams.readFully(in, uploadedParts, offset, partSize);
                    assertThat(in.read(), is(-1));
                }
            }

            return new UploadPartResult();
        }
    }

    private final class CompleteMultipartUploadHandler implements Answer<CompleteMultipartUploadResult> {
        @Override
        public CompleteMultipartUploadResult answer(final InvocationOnMock invocation) throws Throwable {
            final CompleteMultipartUploadRequest request = (CompleteMultipartUploadRequest) invocation.getArguments()[0];

            if (uploadId != null && uploadId.equals(request.getUploadId())) {
                uploadId = null;
            }

            return new CompleteMultipartUploadResult();
        }
    }
}
