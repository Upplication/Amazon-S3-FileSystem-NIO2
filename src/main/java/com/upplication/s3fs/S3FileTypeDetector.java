package com.upplication.s3fs;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.spi.FileTypeDetector;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.upplication.s3fs.util.S3Utils;

public class S3FileTypeDetector extends FileTypeDetector {
	@Override
	public String probeContentType(Path path) throws IOException {
		if (path instanceof S3Path) {
			S3Path s3Path = (S3Path) path;
			ObjectMetadata metadata = S3Utils.getObjectMetadata(s3Path);
			if (metadata != null)
				return metadata.getContentType();
		}
		return null;
	}
}
