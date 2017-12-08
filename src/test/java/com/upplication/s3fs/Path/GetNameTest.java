package com.upplication.s3fs.Path;

import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.assertEquals;

public class GetNameTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    private S3Path getPath(String path) {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNameBucket() {
        // TODO: this is ok?
        S3Path path = getPath("/bucket");
        path.getName(0);
    }

    @Test
    public void getName0() {
        S3Path path = getPath("/bucket/file");
        assertEquals(getPath("/bucket/file"), path.getName(0));
    }


    @Test
    public void getNames() {
        S3Path path = getPath("/bucket/path/to/file");
        assertEquals(path.getName(0), getPath("/bucket/path/"));
        assertEquals(path.getName(1), getPath("to/"));
        assertEquals(path.getName(2), getPath("file"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNameOutOfIndex() {
        S3Path path = getPath("/bucket/path/to/file");
        path.getName(3);
    }
}
