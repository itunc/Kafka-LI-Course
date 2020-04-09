package com.github.itunc.kafka.bcourse.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

  static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

  public static RestHighLevelClient createClient(){

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(
                    System.getenv("ELASITC_BONSAI_USERNAME"),
                    System.getenv("ELASITC_BONSAI_PASSWORD")));
    RestClientBuilder builder = RestClient.builder(
            new HttpHost(System.getenv("ELASITC_BONSAI_HOSTNAME"), 443, "https"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder ->
              httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            );
    RestHighLevelClient client = new RestHighLevelClient(builder);
    return  client;
  }

  public static KafkaConsumer<String, String> createConsumer(String topic){

    // Create Producer Properties
    Properties properties = new Properties();
    String bootstrapServer = "127.0.0.1:9092";
    String groupId  = "kafka-demo-elastcisearch";
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest/none

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Arrays.asList(topic));

    return consumer;
  }

  public static void main(String[] args) throws IOException {
    RestHighLevelClient client = createClient();
    KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

    while (true){
      ConsumerRecords<String, String> records =
              consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records){
        // where we insert data into ElasticSearch
        IndexRequest indexRequest = new IndexRequest("twitter",
                "tweets"
        ).source(record.value(), XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        logger.info("id: " + indexResponse.getId());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    }

    // Close client gracefully
    //client.close();
  }

}
