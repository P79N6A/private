package com.yurun.spark.streaming;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * Created by yurun on 18/1/16.
 *
 * Spark streaming example.
 */
public class SparkStreamingExample {

  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf();

    JavaSparkContext sc = new JavaSparkContext(conf);

    long batch = 1;

    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(batch));

    String kafkaServers = "d013057201.dip.weibo.com:9092,d013057202.dip.weibo.com:9092";
    String group = "yurun";
    String topic = "test";

    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", kafkaServers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", group);
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", true);

    JavaInputDStream<ConsumerRecord<String, String>> stream =
        KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferBrokers(),
            ConsumerStrategies.<String, String>Subscribe(
                Collections.singletonList(topic),
                kafkaParams));

    stream
        .map(ConsumerRecord::value)
        .flatMap(line -> Arrays.asList(line.split(",")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1L))
        .reduceByKey((a, b) -> a + b).print();

    ssc.start();
    ssc.awaitTermination();
  }

}
