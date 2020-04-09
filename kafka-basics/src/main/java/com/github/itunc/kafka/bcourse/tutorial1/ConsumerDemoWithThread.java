package com.github.itunc.kafka.bcourse.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
  public ConsumerDemoWithThread() {

  }

  public static void main(String[] args) {
    new ConsumerDemoWithThread().run();
  }

  private void run(){
    //System.out.println("Hello world!");
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    Properties properties = new Properties();
    String bootstrapServer = "127.0.0.1:9092";
    String groupId  = "my-sixth-application";
    String topic = "first_topic";

    // latch for dealing with multiple threads
    CountDownLatch latch = new CountDownLatch(1);

    // create consumer runnable
    Runnable myConsumerRunnable = new ConsumerRunnable(
            bootstrapServer,
            groupId,
            topic,
            latch
    );

    // start thread
    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread( () -> {
      logger.info("Cought shutdown hook!");
      ((ConsumerRunnable) myConsumerRunnable).shutdown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        logger.error("SD Hook - Application interrupted", e);
      } finally {
        logger.info("SD Hook - Application closed");
      }
    }
    ));

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Application interrupted", e);
    } finally {
      logger.info("Application closing");
    }
  }

  private class ConsumerRunnable implements Runnable {

    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String bootstrapServers,
                            String groupId,
                            String topic,
                            CountDownLatch latch){
      this.latch = latch;

      // properties
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest/non

      // create consumer
      this.consumer = new KafkaConsumer<>(properties);

      // subscribe consumer to our topics
      consumer.subscribe(Collections.singleton(topic));

    }

    @Override
    public void run() {
      // poll for new data
      try {
        while (true) {
          ConsumerRecords<String, String> records =
                  consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord record : records) {
            logger.info("key: " + record.key() + ", value: " + record.value());
            logger.info("partition: " + record.partition() + ", offset: " + record.offset());
          }
        }
      } catch (WakeupException e){
        logger.info("Received shutdown signal!");
      } finally {
        consumer.close();
        // tell our main code we're done with consumer
        latch.countDown();
      }
    }

    public void shutdown() {
      //  wakeup method interrupts consumer.poll)= and it causes the exception WakeUpException
      consumer.wakeup();
    }

  }
}
