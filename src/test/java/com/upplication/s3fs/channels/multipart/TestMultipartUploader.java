package com.upplication.s3fs.channels.multipart;

import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class TestMultipartUploader extends MultipartUploader<TestMultipartUploader.TestChunk> {

    private static final long DEFAULT_PART_SIZE = 16 * 1024 * 1024;

    private final Map<PartKey, Part<TestChunk>> partsUploaded = new HashMap<>();
    private final List<PartKey> transferStartedWithParts = new ArrayList<>();

    private boolean wasTransferStarted;
    private boolean wasTransferEnded;

    TestMultipartUploader(
            Observable<PartKey> changingParts,
            Long partSizeInBytes
    ) {
        super(changingParts, partSizeInBytes);
    }

    TestMultipartUploader(
            Observable<PartKey> changingParts
    ) {
        this(changingParts, DEFAULT_PART_SIZE);
    }

    @Override
    protected void startTransfer(List<PartKey> initialParts) {
        wasTransferStarted = true;
        transferStartedWithParts.addAll(initialParts);
    }

    @Override
    protected Part<TestChunk> uploadNewPart(int partId, PartKey partKey) {
        Part<TestChunk> newPart = new Part<>(partKey, partId, new TestChunk());
        partsUploaded.put(partKey, newPart);
        return newPart;
    }

    @Override
    protected String endTransfer() {
        wasTransferEnded = true;
        return "";
    }

    @RequiredArgsConstructor
    public static class TestChunk {

    }

}
