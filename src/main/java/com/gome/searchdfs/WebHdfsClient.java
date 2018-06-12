package com.gome.searchdfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * @Description:
 * @Author: lipeng-ds
 * @Date: Create in 18-5-4 上午11:05
 * https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
 */

public class WebHdfsClient {

    private String urlFix;
    private CloseableHttpClient httpClient;

    //注意这个地方不能用CloseableHttpClient，会自动关闭
    private HttpClient httpClient_read;
    private HttpResponse readResponse;
    private InputStream inputStream;

    public WebHdfsClient(String server, String port) {
        urlFix = String.format("http://%s:%s/webhdfs/v1", server, port);
        httpClient = HttpClients.createDefault();
        httpClient_read = HttpClients.createDefault();
    }

    //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
    //                    [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
    //                    [&permission=<OCTAL>][&buffersize=<INT>]"
    /**
     * 创建或者写文件
     * @param srcFile
     * @param dstFile
     * @param overwrite
     * @param buffersize
     * @return
     */
    boolean createAndWriteFile(String srcFile, String dstFile, boolean overwrite, int buffersize) {
        //check srcfile if exist
        File file = new File(srcFile);
        if (!file.exists())
            return false;
        String url = String.format("%s%s?op=CREATE&overwrite=%s&buffersize=%d", urlFix, dstFile, overwrite, buffersize);
        HttpPut httpPut = new HttpPut(url);
        try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 307) return false;
            String redireUrl = response.getHeaders("Location")[0].getValue();
            HttpUriRequest put = RequestBuilder.create("PUT").setUri(redireUrl).setEntity(new FileEntity(file)).build();
            try(CloseableHttpResponse redireResponse = httpClient.execute(put)) {
               return redireResponse.getStatusLine().getStatusCode() == 201;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 追加内容
     * @param srcFile
     * @param dstFile
     * @param buffersize
     * @return
     */
    boolean appendFile(String srcFile, String dstFile, int buffersize) {
        File file = new File(srcFile);
        if (!file.exists())
            return false;
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>][&noredirect=<true|false>]"
        String url = String.format("%s%s?op=APPEND&buffersize=%d", urlFix, dstFile, buffersize);
        HttpPost httpPost = new HttpPost(url);
        try(CloseableHttpResponse postResponse = httpClient.execute(httpPost)){
            int statusCode = postResponse.getStatusLine().getStatusCode();
            if (statusCode != 307) return false;
            String redireUrl = postResponse.getHeaders("Location")[0].getValue();
            HttpUriRequest appendRequest = RequestBuilder.create("POST").setUri(redireUrl).setEntity(new FileEntity(file)).build();
            try(CloseableHttpResponse redireResponse = httpClient.execute(appendRequest)) {
                return redireResponse.getStatusLine().getStatusCode() == 200;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    InputStream openAndReadFile(String file, int buffersize) {
        try{
            String url = String.format("%s%s?op=OPEN&buffersize=%d", urlFix, file, buffersize);
            HttpGet httpGet = new HttpGet(url);
            readResponse = httpClient_read.execute(httpGet);
            if (readResponse.getStatusLine().getStatusCode() != 200) return null;
            inputStream = readResponse.getEntity().getContent();
            return inputStream;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void closeRead() {
        try {
            if (readResponse != null)
                HttpClientUtils.closeQuietly(readResponse);
            if (inputStream != null)
                inputStream.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    //curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
    //                              [&recursive=<true|false>]"
    //
    public boolean deleteFileOrDirectory(String path, boolean recursive) {
        String url = String.format("%s%s?op=DELETE&recursive=%s", urlFix, path, recursive);
        HttpDelete httpDelete = new HttpDelete(url);
        try (CloseableHttpResponse delResponse = httpClient.execute(httpDelete)) {
            if (delResponse.getStatusLine().getStatusCode() != 200) return false;
            return getResponsRes(delResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean makeDir(String path) {
        String url = String.format("%s%s?op=MKDIRS", urlFix, path);
        HttpPut httpPut = new HttpPut(url);
        try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
            if (response.getStatusLine().getStatusCode() != 200) return false;
           return getResponsRes(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean getResponsRes(CloseableHttpResponse response) {
        try {

            String source = EntityUtils.toString(response.getEntity());
            JSONObject jsonObject = (JSONObject)JSON.parse(source);
            return jsonObject.getBoolean("boolean");

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
