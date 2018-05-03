package com.yurun.clickhouse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClickHouseQueryMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseQueryMain.class);

  private static String getSQL() throws Exception {
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(
            ClickHouseUpdateMain.class.getClassLoader().getResourceAsStream("query.sql")));

    StringBuilder sb = new StringBuilder();

    String line;

    while ((line = reader.readLine()) != null) {
      sb.append(line).append("\n");
    }

    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
    try {
      String driverName = "ru.yandex.clickhouse.ClickHouseDriver";

      String url = "jdbc:clickhouse://d085126109.aliyun.dip.weibo.com:8123";

      Class.forName(driverName);

      Connection conn = DriverManager.getConnection(url);

      Statement stmt = conn.createStatement();

      ResultSet rs = stmt.executeQuery(getSQL());

      int columns = rs.getMetaData().getColumnCount();

      LOGGER.info("columns: {}", columns);

      while (rs.next()) {
        String[] row = new String[columns];

        for (int index = 1; index <= columns; index++) {
          Object result = rs.getObject(index);

          row[index - 1] = Objects.nonNull(result) ? result.toString() : "NULL";
        }

        LOGGER.info(StringUtils.join(row, ", "));
      }

      rs.close();

      stmt.close();

      conn.close();
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }

}
