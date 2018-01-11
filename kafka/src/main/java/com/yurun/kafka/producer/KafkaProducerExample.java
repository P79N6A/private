package com.yurun.kafka.producer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/1/10.
 *
 * Kafka Producer Example
 */
public class KafkaProducerExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerExample.class);

  public static void main(String[] args) {
    Properties properties = new Properties();

    properties
        .put("bootstrap.servers", "d013057201.dip.weibo.com:9092,d013057202.dip.weibo.com:9092");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<String, String>(properties);

    String topic = "test";

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    int count = 0;
    while (count++ < 1000) {
      String value = sdf.format(new Date());
      producer.send(
          new ProducerRecord<String, String>(topic, value));

      LOGGER.info("producer send: {}", value);

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }

    producer.close();
  }

}
