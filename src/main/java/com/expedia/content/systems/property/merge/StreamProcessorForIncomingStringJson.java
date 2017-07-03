package com.expedia.content.systems.property.merge;

import com.expedia.content.systems.property.model.JoinResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;


import java.util.Iterator;
import java.util.Properties;

public class StreamProcessorForIncomingStringJson {
  private static final String JSON_TOPIC = "incoming-json";
  private static final String JSON_TOPIC2 = "incoming-json2";

  public static void main(String[] args) {
    StreamProcessorForIncomingStringJson processor = new StreamProcessorForIncomingStringJson();
    processor.start();
  }

  private void start() {
    // Setup a builder for the streams
    KStreamBuilder builder = new KStreamBuilder();
    Serde<String> stringSerde = Serdes.String();
    // load a simple json serializer
    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    // load a simple json deserializer
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    // use the simple json serializer and deserialzer we just made and load a Serde for streaming data
    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    KTable<String, JsonNode> rawTable1 = builder.table(stringSerde, jsonSerde, JSON_TOPIC, "json-store1");
    KTable<String, JsonNode> rawTable2 = builder.table(stringSerde, jsonSerde, JSON_TOPIC2, "json-store2");

    KTable<String, JsonNode> intermediateJoin = rawTable1
            .join(rawTable2, JoinResult::new)
            .mapValues(v -> mergeJsonObjects(v.getElement1(), v.getElement2()))
            /*.through("intermediate-results", "intermediate-store")*/;
    intermediateJoin.print(stringSerde, jsonSerde);
    intermediateJoin.to(stringSerde, jsonSerde, "json-merge");

    initStream(builder, stringSerde, jsonSerde);
  }

  private JsonNode mergeJsonObjects(JsonNode mainNode, JsonNode updateNode) {
    Iterator<String> fieldNames = updateNode.fieldNames();

    while (fieldNames.hasNext()) {

      String fieldName = fieldNames.next();
      JsonNode jsonNode = mainNode.get(fieldName);

      if (jsonNode != null) {
        if (jsonNode.isObject()) {
          mergeJsonObjects(jsonNode, updateNode.get(fieldName));
        } else if (jsonNode.isArray()) {
          for (int i = 0; i < jsonNode.size(); i++) {
            mergeJsonObjects(jsonNode.get(i), updateNode.get(fieldName).get(i));
          }
        }
      } else {
        if (mainNode instanceof ObjectNode) {
          // Overwrite field
          JsonNode value = updateNode.get(fieldName);

          if (value.isNull()) {
            continue;
          }

          if (value.isIntegralNumber() && value.toString().equals("0")) {
            continue;
          }

          if (value.isFloatingPointNumber() && value.toString().equals("0.0")) {
            continue;
          }

          ((ObjectNode) mainNode).put(fieldName, value);
        }
      }
    }

    return mainNode;
  }

  private void initStream(KStreamBuilder builder, Serde<String> stringSerde, Serde<JsonNode> jsonNodeSerde) {
    KafkaStreams kafkaStreams = new KafkaStreams(builder, getProperties(jsonNodeSerde));
    kafkaStreams.setUncaughtExceptionHandler((thread, e) -> {
      System.out.println(
              "Kafka Stream Failure! -- ThreadName=" + thread.getName() + " -- ThreadId=" + thread.getId() + " -- Reason =" + e.getMessage());
      e.printStackTrace();
    });
    kafkaStreams.cleanUp();
    kafkaStreams.start();
    System.out.println("Now it's started");

    // Gracefully shutdown on an interrupt
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      kafkaStreams.close();
      jsonNodeSerde.close();
      stringSerde.close();
    }));
  }

  private Properties getProperties(Serde<JsonNode> jsonNodeSerde) {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello-kafka-streams-12");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    return config;
  }
}
