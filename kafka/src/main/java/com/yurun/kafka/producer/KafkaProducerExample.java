package com.yurun.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by yurun on 18/1/10.
 *
 * Kafka Producer Example
 */
public class KafkaProducerExample {

  public static void main(String[] args) {
    Properties properties = new Properties();

    properties
        .put("bootstrap.servers", "d013057201.dip.weibo.com:9092,d013057202.dip.weibo.com:9092");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<String, String>(properties);

    String topic = "test";

    int count = 0;
    while (count++ < 10) {
      producer.send(
          new ProducerRecord<String, String>(topic, String.valueOf(System.currentTimeMillis())));

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }

    producer.close();
  }

}
