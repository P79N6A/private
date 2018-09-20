package com.yurun.test;

/** @author yurun */
public class SplitMain {
  public static void main(String[] args) {
    String line = "1|2";

    String[] words = line.split("\\|", -1);

    System.out.println(words.length);
  }
}
