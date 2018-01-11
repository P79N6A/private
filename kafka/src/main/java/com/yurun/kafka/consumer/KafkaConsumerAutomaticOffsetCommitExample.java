package com.yurun.kafka.consumer;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/1/10.
 *
 * Kafka Consumer Example
 */
public class KafkaConsumerAutomaticOffsetCommitExample {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaConsumerAutomaticOffsetCommitExample.class);

  public static void main(String[] args) {
    Properties properties = new Properties();

    properties
        .put("bootstrap.servers", "d013057201.dip.weibo.com:9092,d013057202.dip.weibo.com:9092");
    properties.put("group.id", "yurun");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties
        .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    consumer.subscribe(Collections.singletonList("test"));

    while (true) {
      ConsumerRecords<String, String> records;

      try {
        records = consumer.poll(100);
      } catch (WakeupException e) {
        break;
      }

      for (ConsumerRecord<String, String> record : records) {
        LOGGER.info("offset = {}, key = {}, value = {}",
            record.offset(), record.key(), record.value());
      }
    }
  }

}


