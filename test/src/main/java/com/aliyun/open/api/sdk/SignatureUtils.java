package com.aliyun.open.api.sdk;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;


public class SignatureUtils {

  private final static String CHARSET_UTF8 = "utf8";
  private final static String ALGORITHM = "UTF-8";
  private final static String SEPARATOR = "&";

  public static Map<String, String> splitQueryString(String url)
      throws URISyntaxException, UnsupportedEncodingException {
    URI uri = new URI(url);
    String query = uri.getQuery();
    final String[] pairs = query.split("&");
    TreeMap<String, String> queryMap = new TreeMap<String, String>();
    for (String pair : pairs) {
      final int idx = pair.indexOf("=");
      final String key = idx > 0 ? pair.substring(0, idx) : pair;
      if (!queryMap.containsKey(key)) {
        queryMap.put(key, URLDecoder.decode(pair.substring(idx + 1), CHARSET_UTF8));
      }
    }
    return queryMap;
  }

  public static String generate(String method, Map<String, String> parameter,
                                String accessKeySecret) throws Exception {
    String signString = generateSignString(method, parameter);
    System.out.println("signString---" + signString);
    byte[] signBytes = hmacSHA1Signature(accessKeySecret + "&", signString);
    String signature = newStringByBase64(signBytes);
    System.out.println("signature---" + signature);
    if ("POST".equals(method)) {
      return signature;
    }
    return URLEncoder.encode(signature, "UTF-8");

  }

  public static String generateSignString(String httpMethod, Map<String, String> parameter)
      throws IOException {
    TreeMap<String, String> sortParameter = new TreeMap<String, String>();
    sortParameter.putAll(parameter);

    String canonicalizedQueryString = generateQueryString(sortParameter, true);
    if (null == httpMethod) {
      throw new RuntimeException("httpMethod can not be empty");
    }

    StringBuilder stringToSign = new StringBuilder();
    stringToSign.append(httpMethod).append(SEPARATOR);
    stringToSign.append(percentEncode("/")).append(SEPARATOR);
    stringToSign.append(percentEncode(canonicalizedQueryString));

    return stringToSign.toString();
  }

  public static String generateQueryString(Map<String, String> params, boolean isEncodeKV) {
    StringBuilder canonicalizedQueryString = new StringBuilder();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      if (isEncodeKV) {
        canonicalizedQueryString.append(percentEncode(entry.getKey())).append("=")
            .append(percentEncode(entry.getValue())).append("&");
      } else {
        canonicalizedQueryString.append(entry.getKey()).append("=")
            .append(entry.getValue()).append("&");
      }
    }
    if (canonicalizedQueryString.length() > 1) {
      canonicalizedQueryString.setLength(canonicalizedQueryString.length() - 1);
    }
    return canonicalizedQueryString.toString();
  }

  public static String percentEncode(String value) {
    try {
      return value == null ? null : URLEncoder.encode(value, CHARSET_UTF8)
          .replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
    } catch (Exception e) {
    }
    return "";
  }

  public static byte[] hmacSHA1Signature(String secret, String baseString)
      throws Exception {
    if (StringUtils.isEmpty(secret)) {
      throw new IOException("secret can not be empty");
    }
    if (StringUtils.isEmpty(baseString)) {
      return null;
    }
    Mac mac = Mac.getInstance("HmacSHA1");
    SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(CHARSET_UTF8), ALGORITHM);
    mac.init(keySpec);
    return mac.doFinal(baseString.getBytes(CHARSET_UTF8));
  }

  public static String newStringByBase64(byte[] bytes)
      throws UnsupportedEncodingException {
    if (bytes == null || bytes.length == 0) {
      return null;
    }

    return new String(Base64.encodeBase64(bytes, false), CHARSET_UTF8);
  }

  public static void main(String[] args) {
    String action = "DescribeCdnDomainLogs";
    String domainName = "us.sinaimg.cn";
    String logDay = "2018-05-14";

    String format = "JSON";
    String version = "2014-11-11";
    String accessKeyId = "LTAIxKzl1eARmD9r";
    String accessKeySecret = "tiPsFAfnqN8cBg2SLiCclQzGf5MpzC";
    String signatureMethod = "HMAC-SHA1";
    String signatureVersion = "1.0";

    Map<String, String> param = new HashMap<String, String>();

    param.put("Action", action);
    param.put("DomainName", domainName);
    param.put("LogDay", logDay);

    param.put("Format", format);
    param.put("Version", version);
    param.put("AccessKeyId", accessKeyId);
    param.put("SignatureMethod", signatureMethod);
    param.put("Timestamp", "2018-05-15T11:00:46Z");
    param.put("SignatureVersion", signatureVersion);
    param.put("SignatureNonce", "4");

    try {
      String url = generate("GET", param, accessKeySecret);
      System.out.println("url: " + url);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }


}