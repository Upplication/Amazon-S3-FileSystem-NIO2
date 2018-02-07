package com.upplication.s3fs.channels.multipart;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
class MultipartUploadSummary {

    private final boolean performed;
    private final long bytesReceived;

}
