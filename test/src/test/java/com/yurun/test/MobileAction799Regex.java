package com.yurun.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** @author yurun */
public class MobileAction799Regex {
  public static void main(String[] args) {
    String line =
        "v5-web-025.mweibo.xxg.intra.weibo.cn|2018-07-20 08:35:46`5776458894`799`2017607:ddb864c276f4d824c0ab55facbf45a24`10000495`102803``10000001```10000001`1082093010`3333_2001`3333_2001`183.236.19.71`0`01ArAJvNjKkwUEA5VMBHFEvL9MwFl4UeVUdgsWQcAvM7kjfjE.`";

    String regex =
        "^([^|]+)\\|([^`]+)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`([^`]*)`(.*)$";

    Pattern pattern = Pattern.compile(regex);

    Matcher matcher = pattern.matcher(line);

    if (matcher.matches()) {
      for (int index = 1; index <= matcher.groupCount(); index++) {
        System.out.println(index + ":\t" + matcher.group(index));
      }
    }
  }
}
