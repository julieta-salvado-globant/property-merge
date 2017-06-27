package com.expedia.content.systems.property.merge;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class StreamProcessor {
  private static final String TITLE_TOPIC = "title";
  private static final String TEST_TOPIC = "test";

  public static void main(String[] args) {
    StreamProcessor processor = new StreamProcessor();
    processor.start();
  }

  private void start() {
    KStreamBuilder builder = new KStreamBuilder();

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello-kafka-streams-9");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    Serde<String> stringSerde = Serdes.String();
    KTable<String, String> titleTable = builder.table(stringSerde, stringSerde, TITLE_TOPIC, "title-store");
    KTable<String, String> testTable = builder.table(stringSerde, stringSerde, TEST_TOPIC, "test-store");
    KTable<String, String> locationTable = builder.table(stringSerde, stringSerde, "location", "location-store");

    KTable<String, String> intermediateJoin = titleTable.join(testTable, (title, test) -> {
      return new StringBuilder()
              .append(title)
              .append(" ")
              .append(test)
              .toString();
    });
    KTable<String, String> intermediateResult = intermediateJoin.through("intermediate-results", "intermediate-store");
    intermediateJoin.print(stringSerde, stringSerde);

    KTable<String, String> finalJoin = locationTable.join(intermediateResult, (title, test) -> {
      return new StringBuilder()
              .append(title)
              .append(" ")
              .append(test)
              .toString();
    });
    finalJoin.to(stringSerde, stringSerde,"final-results");
    finalJoin.print(stringSerde, stringSerde);

    System.out.println("Starting Kafka Streams Example");
    KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
    kafkaStreams.setUncaughtExceptionHandler((thread, e) -> {
      System.out.println(
              "Kafka Stream Failure! -- ThreadName=" + thread.getName() + " -- ThreadId=" + thread.getId() + " -- Reason =" + e.getMessage());
      e.printStackTrace();
    });
    kafkaStreams.cleanUp();
    kafkaStreams.start();
    System.out.println("Now it's started");

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      kafkaStreams.close();
      stringSerde.close();
    }));
  }
}
