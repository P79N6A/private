package com.yurun.kafka;

import java.util.regex.Pattern;

/**
 * Created by yurun on 18/3/8.
 */
public class RegexTest {

  public static void main(String[] args) {
    System.out.println(saneDockerImage("registry.api.weibo.com/dip/nyx-hadoop:2.2_12"));
  }

  private static boolean saneDockerImage(String containerImageName) {

    Pattern dockerImagePattern=Pattern.compile("^(([\\w\\.-/]+)(:\\d+)*\\/)?[\\w\\.:-]+$");

    return dockerImagePattern.matcher(containerImageName).matches();
  }

}
