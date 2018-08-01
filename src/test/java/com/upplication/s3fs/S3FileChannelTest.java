package com.upplication.s3fs;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.*;
import java.util.EnumSet;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class S3FileChannelTest extends S3UnitTestBase {

    private AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();

    @Before
    public void setup() throws IOException {
        FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        reset(client);
    }

    @Test
    public void constructorRead() throws IOException {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.READ));
        assertNotNull(channel);
        channel.read(ByteBuffer.allocate(10));
        channel.close();
    }

    @Test(expected = NonWritableChannelException.class)
    public void constructorReadButTryToWrite() throws IOException {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.READ));
        assertNotNull(channel);
        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();
    }

    @Test
    public void constructorWrite() throws IOException {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE));
        assertNotNull(channel);
        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();
    }

    @Test(expected = NonReadableChannelException.class)
    public void constructorWriteButTryToRead() throws IOException {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE));
        assertNotNull(channel);
        channel.read(ByteBuffer.allocate(10));
        channel.close();
    }

    @Test
    public void readNeedsToCloseChannel() throws IOException {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3FileChannel channel = spy(new S3FileChannel(file1, EnumSet.of(StandardOpenOption.READ)));
        assertNotNull(channel);
        channel.close();

        verify(channel, times(1)).implCloseChannel();
        verify(client, never()).putObject(anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void writeNeedsToCloseChannel() throws IOException {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");

        S3FileChannel channel = spy(new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE)));

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();

        verify(channel, times(1)).implCloseChannel();
        verify(client, times(1)).putObject(argThat(new ArgumentMatcher<PutObjectRequest>() {
            @Override
            public boolean matches(Object argument) {
                PutObjectRequest request = (PutObjectRequest) argument;
                if (!request.getKey().equals("file1")) {
                    return false;
                }
                if (!request.getBucketName().equals("buck")) {
                    return false;
                }
                return true;
            }
        }));
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void alreadyExists() throws IOException {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
    }

    @Test(expected = RuntimeException.class)
    public void brokenNetwork() throws IOException {
        doThrow(new RuntimeException("network broken")).when(client).getObject("buck", "file2");

        S3Path file2 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file2");
        S3FileChannel channel = new S3FileChannel(file2, EnumSet.of(StandardOpenOption.READ));
        channel.close();
    }

    @Test(expected = NoSuchFileException.class)
    public void tempFileDisappeared() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        S3Path file2 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file2");
        S3FileChannel channel = new S3FileChannel(file2, EnumSet.of(StandardOpenOption.WRITE));
        Field f = channel.getClass().getDeclaredField("tempFile");
        f.setAccessible(true);
        Path tempFile = (Path) f.get(channel);
        Files.delete(tempFile);
        channel.close();
    }
}
