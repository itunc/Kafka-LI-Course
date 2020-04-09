package com.github.itunc.kafka.bcourse.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
  public static void main(String[] args) throws ExecutionException, InterruptedException {

    Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    // Create Producer Properties
    Properties properties = new Properties();
    String bootstrapServer = "127.0.0.1:9092";
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());

    // Create Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


    for (int i = 0 ; i < 20; i++) {
      // Create Producer Record
      String topic = "first_topic";
      String value = "hello world " + i + "!";
      String key = "id_" + i;
      ProducerRecord<String, String> record =
              new ProducerRecord<>(topic, key, value);

      logger.info("Key: " + key);

      // Send Data - async
      producer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          // executes at every sent or exception
          if (e == null) {
            logger.info("Received new metadata. \n" +
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());
          } else {
            logger.error("Exception while producing", e);
          }
        }
      }).get(); // block the send - Don't use in production due the poor performance
    }

    // Flush data
    producer.flush();

    // Flush & close producer
    producer.close();

  }
}
