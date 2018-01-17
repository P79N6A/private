package com.yurun.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/1/17.
 *
 * Parse /proc/{pid}/stat
 */
public class ProcStatMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcStatMain.class);

  private static final Pattern PROCFS_STAT_FILE_FORMAT = Pattern.compile(
      "^([0-9-]+)\\s([^\\s]+)\\s[^\\s]\\s([0-9-]+)\\s([0-9-]+)\\s([0-9-]+)\\s" +
          "([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)\\s([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)" +
          "(\\s[0-9-]+){15}");

  private static String getStat(String pid) {
    String stat;

    BufferedReader reader = null;

    try {
      reader = new BufferedReader(
          new InputStreamReader(new FileInputStream("/proc/" + pid + "/stat"),
              CharEncoding.UTF_8));

      stat = reader.readLine();
    } catch (Exception e) {
      stat = null;

      LOGGER.error("read stat file for pid {} error: {}", pid, e.getMessage());
    } finally {
      if (Objects.nonNull(reader)) {
        try {
          reader.close();
        } catch (IOException e) {
          LOGGER.error("close stat file reader for pid {} error: {}", pid, e.getMessage());
        }
      }
    }

    return stat;
  }

  public static void main(String[] args) {
    if (ArrayUtils.isEmpty(args)) {
      return;
    }

    long sumVsize = 0;
    long sumRss = 0;

    for (String pid : args) {
      String stat = getStat(pid);
      if (StringUtils.isEmpty(stat)) {
        LOGGER.warn("pid {} read stat file empty");

        continue;
      }

      Matcher matcher = PROCFS_STAT_FILE_FORMAT.matcher(stat);

      boolean match = matcher.find();

      if (match) {
        // Set(pid) (name) (ppid) (pgrpId) (session) (utime) (stime) (vsize) (rss)
        long vsize = Long.parseLong(matcher.group(10));
        long rss = Long.parseLong(matcher.group(11));

        sumVsize += vsize;
        sumRss += rss;

        LOGGER.info(
            "pid: {}, name: {}, ppid: {}, pgrpId: {}, session: {}, utime: {}, stime: {}, vsize: {}, rss: {}",
            pid,
            matcher.group(2),
            matcher.group(3),
            Integer.parseInt(matcher.group(4)),
            Integer.parseInt(matcher.group(5)),
            Long.parseLong(matcher.group(7)),
            new BigInteger(matcher.group(8)),
            vsize,
            rss);
      } else {
        LOGGER.warn("pid: {} stat not match regex", pid);
      }
    }

    LOGGER.info("sum vsize: {}, sum rss: {}", sumVsize, sumRss);
  }

}
