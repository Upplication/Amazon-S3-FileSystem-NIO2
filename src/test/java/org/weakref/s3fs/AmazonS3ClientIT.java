package org.weakref.s3fs;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertNotNull;
import static org.weakref.s3fs.util.EnvironmentBuilder.getEndpoint;
import static org.weakref.s3fs.util.EnvironmentBuilder.getRealEnv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.weakref.s3fs.util.EnvironmentBuilder;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;

public class AmazonS3ClientIT {
	
	AmazonS3Client client;
	
	@Before
	public void setup() throws IOException{
		// s3client
		final Map<String, Object> credentials = getRealEnv();
		BasicAWSCredentials credentialsS3 = new BasicAWSCredentials(credentials.get(S3FileSystemProvider.ACCESS_KEY).toString(), 
				credentials.get(S3FileSystemProvider.SECRET_KEY).toString());
		AmazonS3 client = new com.amazonaws.services.s3.AmazonS3Client(credentialsS3);
		client.setEndpoint(getEndpoint());
	}
	
	@Test
	public void putObject() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "contenido1".getBytes(), StandardOpenOption.APPEND);
		
		PutObjectResult result = client.putObject(getBucket(), randomUUID().toString(), file.toFile());
	
		assertNotNull(result);
	}
	
	@Test
	public void putObjectWithEndSlash() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "contenido1".getBytes(), StandardOpenOption.APPEND);
		
		PutObjectResult result = client.putObject(getBucket(), randomUUID().toString() + "/", file.toFile());
	
		assertNotNull(result);
	}
	
	@Test(expected = AmazonS3Exception.class)
	public void putObjectWithStartSlash() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "contenido1".getBytes(), StandardOpenOption.APPEND);
		
		client.putObject(getBucket(), "/" + randomUUID().toString(), file.toFile());
	}
	
	@Test(expected = AmazonS3Exception.class)
	public void putObjectWithBothSlash() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "contenido1".getBytes(), StandardOpenOption.APPEND);
		
		PutObjectResult result = client.putObject(getBucket(), "/" + randomUUID().toString() + "/", file.toFile());
	
		assertNotNull(result);
	}
	
	@Test
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