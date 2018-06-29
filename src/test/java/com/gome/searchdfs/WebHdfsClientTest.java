package com.gome.searchdfs;

import org.junit.Before;
import org.junit.Test;

/**
 * @Description:
 * @Author: lipeng-ds
 * @Date: Create in 18-5-4 上午11:38
 */
public class WebHdfsClientTest {
    WebHdfsClient webHdfsClient;

    @Before
    public void init() {
        webHdfsClient = new WebHdfsClient("sbd02", "50070");
    }

    @Test
    public void makeDir() {
        System.out.println(webHdfsClient.makeDir("/gome/hello1/test"));
    }

    @Test
    public void createAndWriteFile() {
        long start = System.currentTimeMillis();
        System.out.println(webHdfsClient.createAndWriteFile("/home/lipeng-ds/software/cn_windows_7_ultimate_with_sp1_x64_dvd_618537.iso", "/gome/hello1/file", true, 1024 * 1024));
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void appendFile() {
        System.out.println(webHdfsClient.appendFile("/home/lipeng-ds/software/test.txt", "/gome/hello1/file",1024 * 1024));
    }

//    @Test
//    public void deleteFileOrDirectory() {
//        System.out.println(webHdfsClient.deleteFileOrDirectory("/gome/hello1/", true));
//    }
}