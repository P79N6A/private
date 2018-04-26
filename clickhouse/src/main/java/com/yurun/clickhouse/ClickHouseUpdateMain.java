package com.yurun.clickhouse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/4/26.
 */
public class ClickHouseUpdateMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseUpdateMain.class);

  private static String getSQL() throws Exception {
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(
            ClickHouseUpdateMain.class.getClassLoader().getResourceAsStream("update.sql")));

    StringBuilder sb = new StringBuilder();

    String line;

    while ((line = reader.readLine()) != null) {
      sb.append(line + "\n");
    }

    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
    try {
      String driverName = "ru.yandex.clickhouse.ClickHouseDriver";

      String url = "jdbc:clickhouse://10.77.114.140:9123";

      Class.forName(driverName);

      Connection conn = DriverManager.getConnection(url);

      Statement stmt = conn.createStatement();

      stmt.execute(getSQL());

      stmt.close();

      conn.close();
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }

}
