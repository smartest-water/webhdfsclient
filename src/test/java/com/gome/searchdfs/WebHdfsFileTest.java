package com.gome.searchdfs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

/**
 * @Description:
 * @Author: lipeng-ds
 * @Date: Create in 18-6-8 下午3:05
 */
public class WebHdfsFileTest {
    private WebHdfsFile webHdfsFile;

    @Before
    public void setUp() {
        webHdfsFile = new WebHdfsFile("sbd02", "50070");
    }

    @Test
    public void write() {
        try {
            webHdfsFile.createAndWriteFile("/gome/hello1/file", true);
            webHdfsFile.writeLine("hello");
            webHdfsFile.writeLine("nihao");
            webHdfsFile.write("gome");
            webHdfsFile.write("gome1");
            webHdfsFile.write("gome2");
            webHdfsFile.flushAndClose();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {

    }

    @Test
    public void readLine() throws IOException {
        if (!webHdfsFile.openAndReadFile("/gome/hello1/file"))
            return;
        while (true) {
            String s = webHdfsFile.readLine();
            if (s == null || s.length() == 0)
                break;
            System.out.println(s);
        }
        webHdfsFile.closeRead();
    }
}