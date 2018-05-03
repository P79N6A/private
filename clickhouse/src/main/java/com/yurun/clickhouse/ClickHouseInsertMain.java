package com.yurun.clickhouse;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/4/26.
 */
public class ClickHouseInsertMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseInsertMain.class);

  public static void main(String[] args) throws Exception {
    try {
      String driverName = "ru.yandex.clickhouse.ClickHouseDriver";

      String url = "jdbc:clickhouse://d085126111.aliyun.dip.weibo.com:8123";

      Class.forName(driverName);

      Connection conn = DriverManager.getConnection(url);

      String sql = "insert into ontime (Year, FlightDate, TailNum) values (?, ?, ?)";

      PreparedStatement stmt = conn.prepareStatement(sql);

      for (int index = 101; index <= 200; index++) {
        stmt.setInt(1, index);
        stmt.setDate(2, new Date(System.currentTimeMillis()));
        stmt.setString(3, String.valueOf(index % 10));

        stmt.addBatch();
      }

      stmt.executeBatch();

      stmt.close();

      conn.close();
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }

}
