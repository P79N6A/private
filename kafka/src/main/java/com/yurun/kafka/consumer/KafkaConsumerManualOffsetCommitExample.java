package com.yurun.kafka.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/1/11.
 *
 * Kafka consumer manual offset commit example.
 */
public class KafkaConsumerManualOffsetCommitExample {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaConsumerManualOffsetCommitExample.class);

  public static void main(String[] args) {
    Properties props = new Properties();

    props.put("bootstrap.servers", "d013057201.dip.weibo.com:9092,d013057202.dip.weibo.com:9092");
    props.put("group.id", "yurun");
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    consumer.subscribe(Collections.singletonList("test"));

    List<String> buffer = new ArrayList<String>();
    int batchSize = 10;

    while (true) {
      ConsumerRecords<String, String> records;

      try {
        records = consumer.poll(100);
      } catch (WakeupException e) {
        break;
      }

      for (ConsumerRecord<String, String> record : records) {
        buffer.add(record.value());
      }

      if (buffer.size() >= batchSize) {
        // process values in buffer
        buffer.clear();

        consumer.commitSync();

        LOGGER.info("kafka consumer commit success");
      }
    }

    consumer.close();
  }

}
