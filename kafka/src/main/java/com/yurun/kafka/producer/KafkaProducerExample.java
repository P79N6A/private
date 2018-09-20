package com.yurun.kafka.producer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/1/10.
 *
 * <p>Kafka Producer Example
 */
public class KafkaProducerExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerExample.class);

  /**
   * Main.
   *
   * @param args no args
   */
  public static void main(String[] args) {
    String kafkaServers = "10.210.77.15:9092";

    String topic = "yurun_test";

    Properties props = new Properties();

    props.put("bootstrap.servers", kafkaServers);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<String, String>(props);

    List<String> domains = new ArrayList<String>();

    domains.add("domain0");
    domains.add("domain1");
    domains.add("domain2");
    domains.add("domain3");
    domains.add("domain4");
    domains.add("domain5");
    domains.add("domain6");
    domains.add("domain7");
    domains.add("domain8");
    domains.add("domain9");

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    Random random = new Random();

    while (true) {
      String value =
          sdf.format(new Date()) + ";" + domains.get(random.nextInt(domains.size())) + ";1";

      producer.send(new ProducerRecord<String, String>(topic, value));

      LOGGER.info("producer send: {}", value);

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }

    producer.close();
  }
}
