package com.upplication.s3fs.channels.multipart;

import io.reactivex.Single;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class MultipartUploaderTest {

    private final Subject<PartKey> incomingParts = ReplaySubject.create();

    private final TestMultipartUploader uploader = new TestMultipartUploader(incomingParts);
    private Single<MultipartUploadSummary> multipartUploadSummary;

    @Before
    public void before() {
        startUpload();
    }

    @Test
    public void doesNotUploadWhenLessThanPartSize() {
        incomingParts.onNext(partInMegs(0, 6));
        incomingParts.onNext(partInMegs(6, 12));
        endUpload();
        assertThat(uploader.isWasTransferStarted()).isFalse();
    }

    @Test
    public void startsTransferAfterReceivingMoreThanOnePartOfData() {
        incomingParts.onNext(partInMegs(0, 12));
        incomingParts.onNext(partInMegs(12, 17));
        endUpload();
        assertThat(uploader.isWasTransferStarted()).isTrue();
    }

    @Test
    public void doesNotStartTransferIfThereIsOnlyOnePartEvenIfGreaterThanChunkSize() {
        incomingParts.onNext(partInMegs(0, 20));
        endUpload();
        assertThat(uploader.isWasTransferStarted()).isFalse();
    }

    @Test
    public void countsBytesSent() {
        incomingParts.onNext(partInMegs(0, 12));
        incomingParts.onNext(partInMegs(12, 24));
        incomingParts.onNext(partInMegs(24, 30));
        assertThat(multipartUploadSummary.blockingGet().getBytesReceived()).isEqualTo(inBytes(30));
    }

    @Test
    public void countsBytesSentAlsoForUploadNotStarted() {
        incomingParts.onNext(partInMegs(0, 12));
        assertThat(multipartUploadSummary.blockingGet().getBytesReceived()).isEqualTo(inBytes(12));
    }

    @Test
    public void sendsLastPart() {
        incomingParts.onNext(partInMegs(0, 20));
        incomingParts.onNext(partInMegs(20, 21));
        endUpload();
        assertThat(uploader.getPartsUploaded().keySet())
                .containsExactly(partInMegs(0, 21));
    }

    private void startUpload() {
        multipartUploadSummary = uploader.upload(incomingParts::onComplete);
    }

    private void endUpload() {
        multipartUploadSummary.blockingGet();
    }

    private PartKey partInMegs(int startInclusive, int endExclusive) {
        return new PartKey(inBytes(startInclusive), inBytes(endExclusive));
    }

    private long inBytes(int mb) {
        return mb * 1024 * 1024;
    }

}