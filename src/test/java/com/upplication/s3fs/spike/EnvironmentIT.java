package com.upplication.s3fs.spike;

import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.junit.Ignore;

import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.util.EnvironmentBuilder;

@Ignore
public class EnvironmentIT {
	public static void main(String[] args) {
		new EnvironmentIT().couldCreateFileSystem();
	}

	public void couldCreateFileSystem(){
		Map<String, Object> res = EnvironmentBuilder.getRealEnv();
		
		assertNotNull(res);
		assertNotNull(res.get(S3FileSystemProvider.ACCESS_KEY));
		assertNotNull(res.get(S3FileSystemProvider.SECRET_KEY));
	}
}
