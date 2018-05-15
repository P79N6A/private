package com.aliyun.open.api.sdk;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import org.apache.commons.codec.CharEncoding;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;

/**
 * @author yurun
 */
public class Download {

  public static void main(String[] args) throws Exception {
    HttpClient client = new HttpClient();

    client.getHttpConnectionManager().getParams()
        .setConnectionTimeout(1000);
    client.getHttpConnectionManager().getParams().setSoTimeout(10000000);

    GetMethod method = new GetMethod("https://cdnlog-sh-public.oss-cn-shanghai.aliyuncs.com/hyjal_ultimate/26431/us.sinaimg.cn/20180514/201805142355-node-aliyun-us.sinaimg.cn-www_spoollxrsaansnq8tjw0_aliyunXweibo.gz?Expires=1526979947&OSSAccessKeyId=LTAIviCc6zy8x3xa&Signature=nBAUORKLXxWjOmTco4K9IOUGojE=");

    client.executeMethod(method);

    GZIPInputStream gzipInputStream = new GZIPInputStream(method.getResponseBodyAsStream());

    BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream, CharEncoding.UTF_8));

    String line = "";

    while ((line = reader.readLine()) != null) {
      System.out.println(line);
    }

    method.releaseConnection();
  }

}
