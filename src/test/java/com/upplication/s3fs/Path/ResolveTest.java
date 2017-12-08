package com.upplication.s3fs.Path;

import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.assertEquals;

public class ResolveTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    private S3Path getPath(String path) {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void resolve() {
        assertEquals(getPath("/bucket/path/to/dir/child/xyz"), getPath("/bucket/path/to/dir/").resolve(getPath("child/xyz")));
        assertEquals(getPath("/bucket/path/to/dir/child/xyz"), getPath("/bucket/path/to/dir/").resolve("child/xyz"));

        assertEquals(getPath("/bucket/path/to/dir/child/xyz"), getPath("/bucket/path/to/dir").resolve(getPath("child/xyz")));
        assertEquals(getPath("/bucket/path/to/dir/child/xyz"), getPath("/bucket/path/to/dir").resolve("child/xyz"));

        assertEquals(getPath("/bucket/path/to/file"), getPath("/bucket/path/to/file").resolve(getPath("")));
        assertEquals(getPath("/bucket/path/to/file"), getPath("/bucket/path/to/file").resolve(""));

        assertEquals(getPath("path/to/file/child/xyz"), getPath("path/to/file").resolve(getPath("child/xyz")));
        assertEquals(getPath("path/to/file/child/xyz"), getPath("path/to/file").resolve("child/xyz"));

        assertEquals(getPath("path/to/file"), getPath("path/to/file").resolve(getPath("")));
        assertEquals(getPath("path/to/file"), getPath("path/to/file").resolve(""));

        assertEquals(getPath("/bucket2/other/child"), getPath("/bucket/path/to/file").resolve(getPath("/bucket2/other/child")));
        assertEquals(getPath("/bucket2/other/child"), getPath("/bucket/path/to/file").resolve("/bucket2/other/child"));
    }


}
