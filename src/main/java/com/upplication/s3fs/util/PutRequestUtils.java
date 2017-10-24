package com.upplication.s3fs.util;
 
 import java.io.InputStream;
 
 import com.amazonaws.services.s3.model.ObjectMetadata;
 import com.amazonaws.services.s3.model.PutObjectRequest;
 import com.amazonaws.services.s3.model.PutObjectResult;
 import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
 import com.amazonaws.util.StringUtils;
 import com.upplication.s3fs.S3FileStore;
 import com.upplication.s3fs.S3Path;
 
 public class PutRequestUtils {
 
 	/**
 	 * Create an putObject request for the bucket represented by the given path see {@link S3Path#getFileStore()} {@link S3FileStore#name()}
 	 * 
 	 * @param path
 	 *            the path
 	 * @param stream
 	 * @param metadata
 	 * @return
 	 */
 	public static PutObjectResult putObjectRequestForPath(S3Path path, InputStream stream, ObjectMetadata metadata) {
 		return putObjectRequestForPath(path, "", stream, metadata);
 	}
 
 	/**
 	 * Create an putObject request for the bucket represented by the given path see {@link S3Path#getFileStore()} {@link S3FileStore#name()}
 	 * The final key is {@link S3Path#getKey()} + addToKey params
 	 * 
 	 * @param path
 	 * @param addToKey
 	 *            the part to be added to the {@link S3Path#getKey()}, (Ex: for reference directories it is necessary add /)
 	 * @param stream
 	 * @param metadata
 	 * @return
 	 */
 	public static PutObjectResult putObjectRequestForPath(S3Path path, String addToKey, InputStream stream, ObjectMetadata metadata) {
 		String bucket = path.getFileStore().name();
 		String key = path.getKey();
 		PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, stream, metadata);
 		if (path.getFileSystem().requiresSseEncrypt()) {
 			String arnKey = path.getFileSystem().getArnKey();
 			SSEAwsKeyManagementParams sseKeyManagementParams = StringUtils.isNullOrEmpty(arnKey) ? new SSEAwsKeyManagementParams() : new SSEAwsKeyManagementParams(arnKey);
 			putObjectRequest = putObjectRequest.withSSEAwsKeyManagementParams(sseKeyManagementParams);
 		}
 		return path.getFileSystem().getClient().putObject(putObjectRequest);
 	}
 
}