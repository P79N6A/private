package com.yurun.spark.sql;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark sql functions.
 *
 * @author yurun
 */
public class SparkSqlFunctions {

  /**
   * Main.
   *
   * @param args no args
   */
  public static void main(String[] args) {
    String master = "local[10]";

    String appName = "SparkSqlFunctions";

    SparkSession session = SparkSession.builder().master(master).appName(appName).getOrCreate();

    session
        .sql("show functions")
        .javaRDD()
        .foreach(
            new VoidFunction<Row>() {
              @Override
              public void call(Row row) {
                System.out.println(row);
              }
            });

    session.stop();
  }
}
