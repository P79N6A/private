package com.yurun.spark.sql;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;

public class SparkWithHiveWarehouse {
  private static List<String> getSqls() throws IOException {
    InputStream input = null;

    try {
      input = SparkWithHiveWarehouse.class.getClassLoader().getResourceAsStream("sqls");

      List<String> lines = IOUtils.readLines(input, CharEncoding.UTF_8);

      return Arrays.asList(StringUtils.join(lines, " ").split(";", -1));
    } finally {
      if (Objects.nonNull(input)) {
        input.close();
      }
    }
  }

  /**
   * Main.
   *
   * @param args no args
   * @throws Exception if error
   */
  public static void main(String[] args) throws Exception {
    SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();

    for (String sql : getSqls()) {
      if (StringUtils.isEmpty(sql)) {
        continue;
      }

      session.sql(sql);
    }

    session.stop();
  }
}
