package com.upplication.s3fs;

import static com.upplication.s3fs.util.EnvironmentBuilder.getRealEnv;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.upplication.s3fs.util.EnvironmentBuilder;

@Ignore
public class AmazonS3ClientIT {
	public static void main(String[] args) throws Exception {
		AmazonS3ClientIT it = new AmazonS3ClientIT();
		it.runTests();
	}
	
	private void runTests() throws Exception {
		setup();
		putObject();
		putObjectWithEndSlash();
		try {
			putObjectWithStartSlash();
			fail("An AmazonS3Exception should've been thrown.");
		} catch(AmazonS3Exception e) {
			// expected
		}
		try {
			putObjectWithBothSlash();
			fail("An AmazonS3Exception should've been thrown.");
		} catch(AmazonS3Exception e) {
			// expected
		}
		putObjectByteArray();
	}
	
	AmazonS3Client client;
	
	@Before
	public void setup() throws IOException{
		// s3client
		final Map<String, Object> credentials = getRealEnv();
		BasicAWSCredentials credentialsS3 = new BasicAWSCredentials(credentials.get(S3FileSystemProvider.ACCESS_KEY).toString(), 
				credentials.get(S3FileSystemProvider.SECRET_KEY).toString());
		AmazonS3 s3 = new com.amazonaws.services.s3.AmazonS3Client(credentialsS3);
		client = new AmazonS3Client(s3);
	}
	
	public void putObject() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);
		
		PutObjectResult result = client.putObject(getBucket(), randomUUID().toString(), file.toFile());
	
		assertNotNull(result);
	}
	
	public void putObjectWithEndSlash() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);
		
		PutObjectResult result = client.putObject(getBucket(), randomUUID().toString() + "/", file.toFile());
	
		assertNotNull(result);
	}
	
	public void putObjectWithStartSlash() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);
		
		client.putObject(getBucket(), "/" + randomUUID().toString(), file.toFile());
	}
	
	public void putObjectWithBothSlash() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);
		
		PutObjectResult result = client.putObject(getBucket(), "/" + randomUUID().toString() + "/", file.toFile());
	
		assertNotNull(result);
	}
	
	public void putObjectByteArray() throws IOException{
		
		PutObjectResult result = client
				.putObject(getBucket(), randomUUID().toString(), new ByteArrayInputStream("contenido1".getBytes()),
						new ObjectMetadata());
	
		assertNotNull(result);
	}
	
	private String getBucket() {
		return EnvironmentBuilder.getBucket().replace("/", "");
	}
}