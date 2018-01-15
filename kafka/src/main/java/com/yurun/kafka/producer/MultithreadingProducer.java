package com.yurun.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/1/15.
 *
 * Multithreading Producer
 */
public class MultithreadingProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultithreadingProducer.class);

  private static class KafkaWriter implements Runnable {

    private Producer<String, String> producer;
    private String topic;
    private AtomicLong counter;

    KafkaWriter(Producer<String, String> producer, String topic, AtomicLong counter) {
      this.producer = producer;
      this.topic = topic;
      this.counter = counter;
    }

    private String getLine() {
      String name = Thread.currentThread().getName();
      String time = String.valueOf(System.currentTimeMillis());

      int count = 10;

      List<String> words = new ArrayList<String>();

      words.add(name);

      words.add(String.valueOf(count));

      for (int index = 0; index < count; index++) {
        words.add(time);
      }

      return StringUtils.join(words, ",");
    }

    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        producer.send(new ProducerRecord<String, String>(topic, getLine()));

        counter.incrementAndGet();
      }
    }

  }

  public static void main(String[] args) {
    String kafkaServers = "d013057201.dip.weibo.com:9092,d013057202.dip.weibo.com:9092";
    String topic = "test";
    int threads = 3;
    long interval = 5 * 1000;

    Properties properties = new Properties();

    properties
        .put("bootstrap.servers", kafkaServers);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<String, String>(properties);

    ExecutorService executors = Executors.newFixedThreadPool(threads);

    AtomicLong counter = new AtomicLong(0);

    for (int index = 0; index < threads; index++) {
      executors.submit(new KafkaWriter(producer, topic, counter));
    }

    while (true) {
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        break;
      }

      LOGGER.info("producer write {} lines/s", counter.getAndSet(0) / 1.0 / (interval / 1000));
    }
  }

}
