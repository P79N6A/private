package com.yurun.kafka.consumer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/1/11.
 *
 * Kafka consumer manual partition offset commit example.
 */
public class KafkaConsumerManualPartitionOffsetCommitExample {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaConsumerManualPartitionOffsetCommitExample.class);

  public static void main(String[] args) {
    Properties props = new Properties();

    props.put("bootstrap.servers", "d013057201.dip.weibo.com:9092,d013057202.dip.weibo.com:9092");
    props.put("group.id", "yurun");
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    consumer.subscribe(Collections.singletonList("test"));

    while (true) {
      ConsumerRecords<String, String> records;

      try {
        records = consumer.poll(100);
      } catch (WakeupException e) {
        break;
      }

      for (TopicPartition partition : records.partitions()) {
        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

        LOGGER.info("partition: {}", partition.partition());

        for (ConsumerRecord<String, String> record : partitionRecords) {
          LOGGER.info("value: {}", record.value());
        }

        long partitionLastOffest = partitionRecords.get(partitionRecords.size() - 1).offset();

        consumer.commitSync(
            Collections.singletonMap(partition, new OffsetAndMetadata(partitionLastOffest + 1)));

        LOGGER.info("partition offset commit");
      }
    }

    consumer.close();
  }

}
