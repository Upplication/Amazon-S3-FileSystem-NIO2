package com.upplication.s3fs;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY;
import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.CONNECTION_TIMEOUT;
import static com.upplication.s3fs.AmazonS3Factory.MAX_CONNECTIONS;
import static com.upplication.s3fs.AmazonS3Factory.MAX_ERROR_RETRY;
import static com.upplication.s3fs.AmazonS3Factory.PROTOCOL;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_DOMAIN;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_HOST;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_PASSWORD;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_PORT;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_USERNAME;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_WORKSTATION;
import static com.upplication.s3fs.AmazonS3Factory.REQUEST_METRIC_COLLECTOR_CLASS;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_RECEIVE_BUFFER_SIZE_HINT;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_SEND_BUFFER_SIZE_HINT;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_TIMEOUT;
import static com.upplication.s3fs.AmazonS3Factory.USER_AGENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.lang.reflect.Field;

import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.upplication.s3fs.util.ExposingAmazonS3Client;
import com.upplication.s3fs.util.ExposingAmazonS3ClientFactory;

public class AmazonS3ClientFactoryTest {
    @Test
    public void neverTrustTheDefaults() {
        AmazonS3ClientFactory clientFactory = new ExposingAmazonS3ClientFactory();
        Properties props = new Properties();
        props.setProperty(ACCESS_KEY, "some_access_key");
        props.setProperty(SECRET_KEY, "super_secret_key");
        props.setProperty(REQUEST_METRIC_COLLECTOR_CLASS, "com.upplication.s3fs.util.NoOpRequestMetricCollector");
        props.setProperty(CONNECTION_TIMEOUT, "10");
        props.setProperty(MAX_CONNECTIONS, "50");
        props.setProperty(MAX_ERROR_RETRY, "3");
        props.setProperty(PROTOCOL, "HTTP");
        props.setProperty(PROXY_DOMAIN, "localhost");
        props.setProperty(PROXY_HOST, "127.0.0.1");
        props.setProperty(PROXY_PASSWORD, "proxy_password");
        props.setProperty(PROXY_PORT, "12345");
        props.setProperty(PROXY_USERNAME, "proxy_username");
        props.setProperty(PROXY_WORKSTATION, "what.does.this.do.localhost");
        props.setProperty(SOCKET_SEND_BUFFER_SIZE_HINT, "48000");
        props.setProperty(SOCKET_RECEIVE_BUFFER_SIZE_HINT, "49000");
        props.setProperty(SOCKET_TIMEOUT, "30");
        props.setProperty(USER_AGENT, "I-am-Groot");
        ExposingAmazonS3Client client = (ExposingAmazonS3Client) clientFactory.getAmazonS3(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);
        AWSCredentialsProvider credentialsProvider = client.getAWSCredentialsProvider();
        AWSCredentials credentials = credentialsProvider.getCredentials();
        assertEquals("some_access_key", credentials.getAWSAccessKeyId());
        assertEquals("super_secret_key", credentials.getAWSSecretKey());
        assertEquals("class com.upplication.s3fs.util.NoOpRequestMetricCollector", client.getRequestMetricsCollector().getClass().toString());
        ClientConfiguration clientConfiguration = client.getClientConfiguration();
        assertEquals(10, clientConfiguration.getConnectionTimeout());
        assertEquals(50, clientConfiguration.getMaxConnections());
        assertEquals(3, clientConfiguration.getMaxErrorRetry());
        assertEquals(Protocol.HTTP, clientConfiguration.getProtocol());
        assertEquals("localhost", clientConfiguration.getProxyDomain());
        assertEquals("127.0.0.1", clientConfiguration.getProxyHost());
        assertEquals("proxy_password", clientConfiguration.getProxyPassword());
        assertEquals(12345, clientConfiguration.getProxyPort());
        assertEquals("proxy_username", clientConfiguration.getProxyUsername());
        assertEquals("what.does.this.do.localhost", clientConfiguration.getProxyWorkstation());
        assertEquals(48000, clientConfiguration.getSocketBufferSizeHints()[0]);
        assertEquals(49000, clientConfiguration.getSocketBufferSizeHints()[1]);
        assertEquals(30, clientConfiguration.getSocketTimeout());
        assertEquals("I-am-Groot", clientConfiguration.getUserAgent());
    }

    @Test
    public void theDefaults() {
    	String[] keys = { com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR, 
    			com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR,
    			com.amazonaws.SDKGlobalConfiguration.ALTERNATE_ACCESS_KEY_ENV_VAR,
    			com.amazonaws.SDKGlobalConfiguration.ALTERNATE_SECRET_KEY_ENV_VAR
    	};
    	Map<String, String> savedVars = clearEnvironmentVars(keys);
        AmazonS3ClientFactory clientFactory = new ExposingAmazonS3ClientFactory();
        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, "giev.ma.access!");
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, "I'll never teeeeeellllll!");
        Properties props = new Properties();
        ExposingAmazonS3Client client = (ExposingAmazonS3Client) clientFactory.getAmazonS3(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);
        AWSCredentialsProvider credentialsProvider = client.getAWSCredentialsProvider();
        AWSCredentials credentials = credentialsProvider.getCredentials();
        assertEquals("giev.ma.access!", credentials.getAWSAccessKeyId());
        assertEquals("I'll never teeeeeellllll!", credentials.getAWSSecretKey());
        assertNull(client.getRequestMetricsCollector());
        setEnvironmentVars(savedVars);
        ClientConfiguration clientConfiguration = client.getClientConfiguration();
        assertEquals(ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT, clientConfiguration.getConnectionTimeout());
        assertEquals(ClientConfiguration.DEFAULT_MAX_CONNECTIONS, clientConfiguration.getMaxConnections());
        assertEquals(-1, clientConfiguration.getMaxErrorRetry());
        assertEquals(Protocol.HTTPS, clientConfiguration.getProtocol());
        assertNull(clientConfiguration.getProxyDomain());
        assertNull(clientConfiguration.getProxyHost());
        assertNull(clientConfiguration.getProxyPassword());
        assertEquals(-1, clientConfiguration.getProxyPort());
        assertNull(clientConfiguration.getProxyUsername());
        assertNull(clientConfiguration.getProxyWorkstation());
        assertEquals(0, clientConfiguration.getSocketBufferSizeHints()[0]);
        assertEquals(0, clientConfiguration.getSocketBufferSizeHints()[1]);
        assertEquals(50000, clientConfiguration.getSocketTimeout());
        assertTrue(clientConfiguration.getUserAgent().startsWith("aws-sdk-java"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void halfTheCredentials() {
        AmazonS3ClientFactory clientFactory = new ExposingAmazonS3ClientFactory();
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, "I'll never teeeeeellllll!");
        Properties props = new Properties();
        props.setProperty(ACCESS_KEY, "I want access");
        clientFactory.getAmazonS3(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);
    }

    @Test(expected = IllegalArgumentException.class)
    public void theOtherHalf() {
        AmazonS3ClientFactory clientFactory = new ExposingAmazonS3ClientFactory();
        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, "I want access");
        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "I'll never teeeeeellllll!");
        clientFactory.getAmazonS3(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongMetricsCollector() {
        AmazonS3ClientFactory clientFactory = new ExposingAmazonS3ClientFactory();
        Properties props = new Properties();
        props.setProperty(ACCESS_KEY, "I want access");
        props.setProperty(SECRET_KEY, "I'll never teeeeeellllll!");
        props.setProperty(REQUEST_METRIC_COLLECTOR_CLASS, "com.upplication.s3fs.util.WrongRequestMetricCollector");
        clientFactory.getAmazonS3(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);
    }

    @Test
    public void defaultSendBufferHint() {
        AmazonS3ClientFactory clientFactory = new ExposingAmazonS3ClientFactory();
        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, "giev.ma.access!");
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, "I'll never teeeeeellllll!");
        Properties props = new Properties();
        props.setProperty(SOCKET_SEND_BUFFER_SIZE_HINT, "12345");
        ExposingAmazonS3Client client = (ExposingAmazonS3Client) clientFactory.getAmazonS3(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);
        ClientConfiguration clientConfiguration = client.getClientConfiguration();
        assertEquals(12345, clientConfiguration.getSocketBufferSizeHints()[0]);
        assertEquals(0, clientConfiguration.getSocketBufferSizeHints()[1]);
    }

    @Test
    public void defaultReceiveBufferHint() {
        AmazonS3ClientFactory clientFactory = new ExposingAmazonS3ClientFactory();
        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, "giev.ma.access!");
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, "I'll never teeeeeellllll!");
        Properties props = new Properties();
        props.setProperty(SOCKET_RECEIVE_BUFFER_SIZE_HINT, "54321");
        ExposingAmazonS3Client client = (ExposingAmazonS3Client) clientFactory.getAmazonS3(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);
        ClientConfiguration clientConfiguration = client.getClientConfiguration();
        assertEquals(0, clientConfiguration.getSocketBufferSizeHints()[0]);
        assertEquals(54321, clientConfiguration.getSocketBufferSizeHints()[1]);
    }

    @Test
    public void overrideHostAndPort() throws Exception {
        AmazonS3ClientFactory clientFactory = new ExposingAmazonS3ClientFactory();
        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, "test");
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, "test");
        ExposingAmazonS3Client client = (ExposingAmazonS3Client) clientFactory.getAmazonS3(URI.create("s3://localhost:8001/"), new Properties());
        URI endpoint = client.getEndpoint();
        assertEquals("https", endpoint.getScheme());
        assertEquals("localhost", endpoint.getHost());
        assertEquals(8001, endpoint.getPort());
    }

    /*
     * This is a helper function for clearEnvironmentVars and setEnvironmentVars.  It returns
     * a map of the environment variables that can be modified for the purposes of setting
     * initial conditions for unit testing.
     * 
     * This was tested on OS X.  The info below on stackoverflow indicated different approaches were needed on different
     * systems.  Therefore I left a 2nd version of the function in the code in case someone finds a problem on a
     * different system.  The second version may provide a jump start on a solution.  Both versions worked on
     * OS X.
     * 
     * This is based on info found in:
     * http://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java/42964302#42964302
     * 
     * With some additional useful reference info:
     * http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/6-b14/java/lang/ProcessEnvironment.java
     * http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/8u40-b25/java/util/Collections.java?av=f#1439
     */
    private Map<String, String> getModifiableEnvironmentMap() {
    	try {
	    	Map<String,String> unmodifiableEnv = System.getenv();
	    	Class<?> cl = unmodifiableEnv.getClass();
	    	Field field = cl.getDeclaredField("m");
	    	field.setAccessible(true);
	    	Map<String,String> modifiableEnv = (Map<String,String>) field.get(unmodifiableEnv);
	    	return modifiableEnv;
    	} catch(Exception e) {
    		throw new RuntimeException("Unable to access writable environment variable map.");
    	}
    }

    /*
     * A second approach to getModifiableEnvironmentMap above.  This is included in case the first method
     * has problems on a different system.
     */
    private Map<String, String> getModifiableEnvironmentMap2() {
    	try {
        	Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
    		Field theUnmodifiableEnvironmentField = processEnvironmentClass.getDeclaredField("theUnmodifiableEnvironment");
    		theUnmodifiableEnvironmentField.setAccessible(true);
    		Map<String,String> theUnmodifiableEnvironment = (Map<String,String>)theUnmodifiableEnvironmentField.get(null);
    		
    		Class<?> theUnmodifiableEnvironmentClass = theUnmodifiableEnvironment.getClass();
    		Field theModifiableEnvField = theUnmodifiableEnvironmentClass.getDeclaredField("m");
    		theModifiableEnvField.setAccessible(true);
    		Map<String,String> modifiableEnv = (Map<String,String>) theModifiableEnvField.get(theUnmodifiableEnvironment);
	    	return modifiableEnv;
    	} catch(Exception e) {
    		throw new RuntimeException("Unable to access writable environment variable map.");
    	}
    }
    
    /**
     * Deletes all the elements matching an element of keys from the environment variable 
     * map for this process (does not effect other processes in the system).  A map of 
     * saved values is returned so deleted elements can be restored later.
     * 
     * @param keys An array of strings for elements that should deleted from environment variables.
     * @return A map of (key, value) pairs that were deleted from the environment variables map.
     */
    private Map<String, String> clearEnvironmentVars(String[] keys) {
    	Map<String,String> modifiableEnv = getModifiableEnvironmentMap();
    	
    	HashMap<String, String> savedVals = new HashMap<String, String>();
    	
    	for(String k : keys) {
    		String val = modifiableEnv.remove(k);
    		if (val != null) { savedVals.put(k, val); }
    	}
    	return savedVals;
    }
    
    /**
     * Adds (key, value) pairs to the environment variable map for this process.
     * If an entry with key already exists the value will be overwritten.
     * 
     * @param varMap A map of all (key, value) pairs to be
     *               added to the environment variable map.
     */
    private void setEnvironmentVars(Map<String, String> varMap) {
    	getModifiableEnvironmentMap().putAll(varMap);
    	
    }
}