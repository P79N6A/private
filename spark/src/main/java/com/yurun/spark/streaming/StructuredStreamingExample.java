package com.yurun.spark.streaming;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Structured streaming example.
 *
 * @author yurun
 */
public class StructuredStreamingExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(StructuredStreamingExample.class);

  private static class QueryListener extends StreamingQueryListener {
    @Override
    public void onQueryStarted(QueryStartedEvent queryStartedEvent) {}

    @Override
    public void onQueryProgress(QueryProgressEvent queryProgressEvent) {
      LOGGER.info(queryProgressEvent.progress().json());
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent queryTerminatedEvent) {}
  }

  private static class TruncateWithMinutes implements UDF2<String, Integer, String> {
    private static final FastDateFormat DATE_FORMAT =
        FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    @Override
    public String call(String timestamp, Integer minutes) throws Exception {
      Calendar c = Calendar.getInstance();

      c.setTime(DATE_FORMAT.parse(timestamp));

      c.set(Calendar.MINUTE, c.get(Calendar.MINUTE) / minutes * minutes);
      c.set(Calendar.SECOND, 0);

      return DATE_FORMAT.format(c.getTime());
    }
  }

  private static class SinkWriter extends ForeachWriter<Row> {
    private long partitionId;
    private long batchId;

    @Override
    public boolean open(long partitionId, long batchId) {
      this.partitionId = partitionId;
      this.batchId = batchId;

      return true;
    }

    @Override
    public void process(Row row) {}

    @Override
    public void close(Throwable throwable) {}
  }

  /**
   * Main.
   *
   * @param args no args
   * @throws Exception if error
   */
  public static void main(String[] args) throws Exception {
    String master = "local[20]";

    String appName = "StructuredStreamingExample";

    String kafkaServers = "10.210.77.15:9092";
    String topic = "yurun_test";

    SparkSession session = SparkSession.builder().master(master).appName(appName).getOrCreate();

    session.streams().addListener(new QueryListener());

    session
        .udf()
        .register("truncate_with_minutes", new TruncateWithMinutes(), DataTypes.StringType);

    Dataset<Row> df =
        session
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaServers)
            .option("subscribe", topic)
            .load();

    df.createOrReplaceTempView("kafka_table");

    List<StructField> fields = new ArrayList<>();

    fields.add(DataTypes.createStructField("logtime", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("domain", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("hit", DataTypes.StringType, false));

    StructType schema = DataTypes.createStructType(fields);

    ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);

    Dataset<Row> rowDf =
        session
            .sql("select cast(value as string) as value from kafka_table")
            .as(Encoders.STRING())
            .map(
                new MapFunction<String, Row>() {
                  @Override
                  public Row call(String line) {
                    String[] words = line.split(";", -1);

                    return RowFactory.create(words[0], words[1], words[2]);
                  }
                },
                encoder);

    rowDf.createOrReplaceTempView("source_table");

    Dataset<Row> selectDf =
        session.sql(
            "select current_timestamp() as timestamp, "
                + "truncate_with_minutes(logtime, 5) as logtime, "
                + "domain, cast(hit as int) as hit "
                + "from source_table");

    Dataset<Row> resultDf =
        selectDf
            .withWatermark("timestamp", "1 minutes")
            .groupBy(
                functions.window(functions.col("timestamp"), "1 minutes"),
                functions.col("logtime"),
                functions.col("domain"))
            .sum("hit");

    StreamingQuery query =
        resultDf
            .writeStream()
            .queryName("query")
            .format("console")
            .outputMode("update")
            .trigger(Trigger.ProcessingTime("10 seconds"))
            .start();

    query.awaitTermination();
  }
}
