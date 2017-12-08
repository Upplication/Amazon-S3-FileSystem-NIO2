package com.upplication.s3fs.spike;

import com.google.common.base.Splitter;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.List;

public class URISpikeTest {

    private List<String> s3Path_cases = Arrays.asList(
            "/bucket//folder//folder2/file",
            "///bucket//folder//folder2/file",
            "//bucket//folder//folder2/file",
            "////bucket//folder//folder2/file",
            "folder//folder2/file",
            "folder//..//./folder2/file/.",
            "folder/folder2/folder3/");

    @Test
    public void test() throws MalformedURLException {
        for (String s3case : s3Path_cases) {
            System.out.println(URI.create(s3case).toString() +
                    " ==> " + URI.create(s3case).normalize().toString());

            System.out.println("Host:" + URI.create(s3case).getHost());
            System.out.println("Raw query:" + URI.create(s3case).getRawQuery());
            System.out.println("Authority:" + URI.create(s3case).getAuthority());
            System.out.println("Query:" + URI.create(s3case).getQuery());
            System.out.println("Raw Path:" + URI.create(s3case).getRawPath());
            System.out.println("Path:" + URI.create(s3case).getPath());

            // get bucket

            System.out.println("Bucket:" + Splitter.on("/").omitEmptyStrings().splitToList(URI.create(s3case).normalize().toString()).get(0));
            System.out.println("Parent:" + URI.create(s3case).resolve("..").toString());
        }


    }

    @Test
    public void uriWithSpaces() throws MalformedURLException {
        URI uri = FileSystems.getDefault().getPath("/file with spaces").toUri();
        System.out.println(uri);
    }

}
