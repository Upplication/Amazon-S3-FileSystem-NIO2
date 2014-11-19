package com.upplication.s3fs;

public class S3FileSystemException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public S3FileSystemException() {
		//
	}

	public S3FileSystemException(Throwable cause) {
		super(cause);
	}

	public S3FileSystemException(String message, Throwable cause) {
		super(message, cause);
	}
}