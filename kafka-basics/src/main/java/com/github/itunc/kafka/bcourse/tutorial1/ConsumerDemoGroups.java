package com.github.itunc.kafka.bcourse.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
  public static void main(String[] args) {
    //System.out.println("Hello world!");
    Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);


    // Create Producer Properties
    Properties properties = new Properties();
    String bootstrapServer = "127.0.0.1:9092";
    String groupId  = "my-fifth-application";
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest/none

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    // subscribe consumer to our topics
    //consumer.subscribe(Arrays.asList("first_topic", "second_topic"));
    String topic = "first_topic";
    consumer.subscribe(Collections.singleton(topic));

    // poll for new data
    while (true){
      ConsumerRecords<String, String> records =
              consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord record : records) {
        logger.info("key: " + record.key() + ", value: " + record.value());
        logger.info("partition: " + record.partition() + ", offset: " + record.offset());
      }
    }



  }
}
