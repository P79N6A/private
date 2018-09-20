package com.yurun.tsp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToInfluxdb {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToInfluxdb.class);

  private static final String zooKeeper =
      "first.zookeeper.dip.weibo.com:2181"
          + ","
          + "second.zookeeper.dip.weibo.com:2181"
          + ","
          + "third.zookeeper.dip.weibo.com:2181"
          + "/kafka/k1001";
  private static final String groupId = "yurun_test";
  private static final String topic = "dip_alarm";
  private static int threads = 1;

  private static final String INFLUXDB_URL = "http://10.210.136.92:8086";
  private static final String DB_NAME = "time_series_prediction";

  private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
    Properties props = new Properties();

    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("auto.offset.reset", "largest");

    return new ConsumerConfig(props);
  }

  public static class ConsumerClient implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerClient.class);

    private KafkaStream<byte[], byte[]> stream;
    private int threadNumber;

    public ConsumerClient(KafkaStream<byte[], byte[]> stream, int threadNumber) {
      this.stream = stream;
      this.threadNumber = threadNumber;
    }

    /** Consumer client run. */
    public void run() {
      LOGGER.info("thread {} started", threadNumber);

      InfluxDB client = null;

      try {
        client = InfluxDBFactory.connect(INFLUXDB_URL);
        client.setDatabase(DB_NAME);

        for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
          String msg = new String(messageAndMetadata.message());



          // test output
          System.out.println(msg);
        }

        LOGGER.info("thread {} stoped", threadNumber);
      } catch (Exception e) {
        LOGGER.error("consumer client run exception: {}" + ExceptionUtils.getFullStackTrace(e));
      } finally {
        if (Objects.nonNull(client)) {
          client.close();
        }
      }
    }
  }

  /**
   * Main.
   *
   * @param args no params
   */
  public static void main(String[] args) {
    if (args.length > 0) {
      threads = Integer.valueOf(args[0]);
    }
    LOGGER.info("Thread number: " + threads);

    final ConsumerConnector consumer =
        kafka.consumer.Consumer.createJavaConsumerConnector(
            createConsumerConfig(zooKeeper, groupId));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, threads);

    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        consumer.createMessageStreams(topicCountMap);

    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

    ExecutorService executor = Executors.newFixedThreadPool(threads);

    int threadNumber = 0;
    for (KafkaStream<byte[], byte[]> stream : streams) {
      executor.submit(new ConsumerClient(stream, threadNumber));
      threadNumber++;
    }

    executor.shutdown();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              public void run() {
                consumer.shutdown();
              }
            });
  }
}
