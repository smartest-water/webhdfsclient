package com.gome.searchdfs;

/**
 * @Description:
 * @Author: lipeng-ds
 * @Date: Create in 18-6-28 下午6:35
 */
public class MainApp {
    public static void main(String[] args) {
        WebHdfsClient webHdfsClient = new WebHdfsClient("sbd02", "50070");
        long start = System.currentTimeMillis();
        webHdfsClient.createAndWriteFile("/app/ideaIC163-15529.zip", "/gome/hello1/file", true, 1024 * 1024);
        System.out.println(System.currentTimeMillis() - start);
    }
}
