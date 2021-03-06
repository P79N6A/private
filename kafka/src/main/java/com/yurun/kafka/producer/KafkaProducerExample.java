package com.yurun.kafka.producer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.commons.lang.StringUtils;
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

    String topic = "hubble_source";

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

    SimpleDateFormat logtimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    List<String> types = new ArrayList<String>();

    types.add("娱乐");
    types.add("体育");
    types.add("教育");
    types.add("财经");
    types.add("社会");
    types.add("其它");

    List<String> catons = new ArrayList<String>();

    catons.add("true");
    catons.add("false");

    Random random = new Random();

    while (true) {
      List<String> words = new ArrayList<String>();

      words.add(logtimeFormat.format(new Date()));
      words.add("1");
      words.add(types.get(random.nextInt(types.size())));
      words.add(catons.get(random.nextInt(catons.size())));

      String value = StringUtils.join(words, "\t");

      producer.send(new ProducerRecord<String, String>(topic, value));

      LOGGER.info("producer send: {}", value);

      try {
        Thread.sleep((int) (Math.random() * 100));
      } catch (InterruptedException e) {
        break;
      }
    }

    producer.close();
  }
}
