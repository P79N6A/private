package com.yurun.spark.streaming;

import java.util.Collections;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

/**
 * Created by yurun on 18/1/16.
 *
 * Spark streaming example.
 */
public class SparkStreamingExample {

  public static void main(String[] args) throws InterruptedException {
    String master = "local[10]";

    String appName = "example";

    long batch = 1;

    String zkQuorums = "d013004044.hadoop.dip.weibo.com:2181/kafka_intra";

    String consumerGroup = "yurun_test";

    String topic = "yurun_1";
    int threads = 1;

    SparkConf conf = new SparkConf();

    conf.setMaster(master);
    conf.setAppName(appName);

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(batch));

    @SuppressWarnings("deprecation")
    JavaDStream<String> stream = KafkaUtils
        .createStream(ssc, zkQuorums, consumerGroup, Collections.singletonMap(topic, threads))
        .map(Tuple2::_2);

    stream.foreachRDD(rdd -> rdd.foreachPartition(iter -> {
      while (iter.hasNext()) {
        System.out.println(iter.next());
      }
    }));

    ssc.start();
    ssc.awaitTermination();
  }

}
