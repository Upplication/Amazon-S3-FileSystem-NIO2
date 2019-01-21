package com.upplication.s3fs;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;

public class S3SeekableByteChannelTest extends S3UnitTestBase {

    @Before
    public void setup() throws IOException {
        FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void constructor() throws IOException {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3SeekableByteChannel channel = new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ));
        assertNotNull(channel);
        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();
    }

    @Test
    public void readDontNeedToSyncTempFile() throws IOException {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3SeekableByteChannel channel = spy(new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.READ)));
        assertNotNull(channel);
        channel.close();

        verify(channel, never()).sync();
    }

    @Test
    public void writeNeedToSyncTempFile() throws IOException {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");

        S3SeekableByteChannel channel = spy(new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE)));

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();

        verify(channel, times(1)).sync();
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void alreadyExists() throws IOException {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
    }

    @Test(expected = RuntimeException.class)
    public void brokenNetwork() throws IOException {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        doThrow(new RuntimeException("network broken")).when(client).getObject("buck", "file2");

        S3Path file2 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file2");
        S3SeekableByteChannel channel = new S3SeekableByteChannel(file2, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ));
        channel.close();
    }

    @Test(expected = NoSuchFileException.class)
    public void tempFileDisappeared() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        S3Path file2 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file2");
        S3SeekableByteChannel channel = new S3SeekableByteChannel(file2, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ));
        Field f = channel.getClass().getDeclaredField("tempFile");
        f.setAccessible(true);
        Path tempFile = (Path) f.get(channel);
        Files.delete(tempFile);
        channel.close();
    }

    @Test
    public void writeFileWithReallyLongName() throws IOException {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        String longDirectoryName = "FuscetellusodiodapibusidfermentumquissuscipitideratEtiamquisquamVestibulumeratnullaullamcorpernecrutrumnonnon";
        String longFileName = "ummyaceratSedutperspiciatisundeomnisisfasdfasdfasfsafdtenatuserrorsitvoluptatemaccusantiumdoloremquelaudantiumtotamremaperiameaqueipsaq";
        client.bucket("buck").file(longDirectoryName + "/" + longFileName);

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/"+ longDirectoryName + "/" + longFileName);
        S3SeekableByteChannel channel = spy(new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE)));
        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();

        verify(channel, times(1)).sync();
    }
}