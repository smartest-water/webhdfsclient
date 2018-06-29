package com.gome.searchdfs;

import java.io.*;

/**
 * @Description:
 * @Author: lipeng-ds
 * @Date: Create in 18-6-8 下午1:40
 */
public class WebHdfsFile {

    private WebHdfsClient webHdfsClient;
    private int buffersize = 1024 * 4;
    //读文件相关
    private boolean overwrite = true;
    private BufferedWriter bufferedWriter;
    private String tmpFile;
    private String dstFile;
    private File file;
    private FileOutputStream fileOutputStream;
    private OutputStreamWriter streamWriter;
    //写文件相关
    private BufferedReader bufferedReader;

    public WebHdfsFile(String server, String port) {
        webHdfsClient = new WebHdfsClient(server, port);
    }

    public void setBuffersize(int buffersize) {
        this.buffersize = buffersize;
    }

    public void createAndWriteFile(String dstFile, boolean overwrite) throws FileNotFoundException {

        this.overwrite = overwrite;
        this.dstFile = dstFile;
        long currentTime = System.currentTimeMillis();
        tmpFile = String.format("/tmp/%d_%d", currentTime, this.hashCode());

        file = new File(tmpFile);
        fileOutputStream = new FileOutputStream(file);
        streamWriter = new OutputStreamWriter(this.fileOutputStream);
        bufferedWriter = new BufferedWriter(streamWriter);
    }

    public void writeLine(String content) throws IOException {
        bufferedWriter.write(content);
        bufferedWriter.newLine();
    }

    public void write(String content) throws IOException {
        bufferedWriter.write(content);
    }

    public void flushAndClose(){
        try {
            bufferedWriter.close();
            streamWriter.close();
            fileOutputStream.close();
            webHdfsClient.createAndWriteFile(tmpFile, dstFile, overwrite, buffersize);
            file.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean openAndReadFile(String file) {
        InputStream inputStream = webHdfsClient.openAndReadFile(file, buffersize);
        if (inputStream == null)
            return false;
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        return true;
    }

    public String readLine() throws IOException {
        return bufferedReader.readLine();
    }

    public void closeRead() {
        try {
            if (bufferedReader != null)
                bufferedReader.close();
            webHdfsClient.closeRead();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public WebHdfsClient getWebHdfsClient() {
        return webHdfsClient;
    }

}
