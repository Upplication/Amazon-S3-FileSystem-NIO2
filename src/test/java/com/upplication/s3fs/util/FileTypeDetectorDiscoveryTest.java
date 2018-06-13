package com.upplication.s3fs.util;

import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FileTypeDetectorDiscoveryTest {

    @Test
    public void testFileDetectorDiscovery() throws IOException, URISyntaxException {

        URL resource = getClass().getClassLoader().getResource("log4j.properties");

        assertNotNull(resource);

        Path path = Paths.get(resource.toURI());
        assertTrue(Files.exists(path));

        String s = Files.probeContentType(path);
        assertEquals("text/plain", s);
        System.out.println();

    }

}
