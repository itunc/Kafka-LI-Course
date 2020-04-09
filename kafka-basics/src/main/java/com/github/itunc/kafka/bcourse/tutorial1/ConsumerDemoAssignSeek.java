package com.github.itunc.kafka.bcourse.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
  public static void main(String[] args) {
    //System.out.println("Hello world!");
    Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);


    // Create Producer Properties
    Properties properties = new Properties();
    String bootstrapServer = "127.0.0.1:9092";
    String groupId  = "my-seventh-application";
    String topic = "first_topic";
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest/none

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    // Assign and seek are mostly used to replay data or fetch a specific message
    // assign
    TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);

    // subscribe consumer to our topics
    //consumer.subscribe(Arrays.asList("first_topic", "second_topic"));
    //consumer.subscribe(Collections.singleton(topic));
    consumer.assign(Arrays.asList(partitionToReadFrom));

    // seek
    long offsetToReadFrom = 15L;
    consumer.seek(partitionToReadFrom, offsetToReadFrom);

    int nummberOfMessagesToRead = 5 ;
    boolean keepReading = true;
    int numberOfMessageSoFar = 0;

    // poll for new data
    while (keepReading){
      ConsumerRecords<String, String> records =
              consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord record : records) {
        numberOfMessageSoFar += 1;
        logger.info("key: " + record.key() + ", value: " + record.value());
        logger.info("partition: " + record.partition() + ", offset: " + record.offset());
        if (numberOfMessageSoFar >= nummberOfMessagesToRead){
          keepReading = false;
          break;
        }
      }
    }
  }
}
