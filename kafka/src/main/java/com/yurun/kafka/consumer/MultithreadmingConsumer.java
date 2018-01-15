package com.yurun.kafka.consumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/1/15.
 */
public class MultithreadmingConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultithreadmingConsumer.class);

  private static class KafkaReader implements Runnable {

    private Properties properties;
    private String topic;
    private AtomicLong counter;

    private long timeout = 100;

    KafkaReader(Properties properties, String topic, AtomicLong counter) {
      this.properties = properties;
      this.topic = topic;
      this.counter = counter;
    }

    public void run() {
      Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

      consumer.subscribe(Collections.singletonList(topic));

      while (true) {
        counter.addAndGet(consumer.poll(timeout).count());
      }
    }

  }

  public static void main(String[] args) {
    String kafkaServers = "d013057201.dip.weibo.com:9092,d013057202.dip.weibo.com:9092";
    String group = "yurun";
    String topic = "test";

    int threads = 3;

    Properties properties = new Properties();

    properties
        .put("bootstrap.servers", kafkaServers);
    properties.put("group.id", group);
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties
        .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    ExecutorService executors = Executors.newFixedThreadPool(threads);

    AtomicLong counter = new AtomicLong();


  }

}
