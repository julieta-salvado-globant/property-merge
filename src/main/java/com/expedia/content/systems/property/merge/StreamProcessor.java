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
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello-kafka-streams-7");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//    config.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    Serde<String> stringSerde = Serdes.String();
    KTable<String, String> titleTable = builder.table(stringSerde, stringSerde, TITLE_TOPIC, "title-store");
    KTable<String, String> testTable = builder.table(stringSerde, stringSerde, TEST_TOPIC, "test-store");

//    KTable<String, Hotel> sinkResult = titleTable.join(testTable, (title, test) -> {
//      return new Hotel.HotelBuilder()
//              .title(title)
//              .test(test)
//              .build();
//    });

    KTable<String, String> sinkResult = titleTable.join(testTable, (title, test) -> {
      return new StringBuilder()
              .append(title)
              .append(" ")
              .append(test)
              .toString();
    });
    sinkResult.to(stringSerde, stringSerde,"final-results");
    sinkResult.print(stringSerde, stringSerde);

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
