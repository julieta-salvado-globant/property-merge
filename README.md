# property-merge

The idea of this POC is to research Kafka Streams and figure out if they are suitable for the task. We have property field updates coming in the form of Kafka topics. After one field changes, we want to merge all fields for that property (avoiding going to db to get those values) and update the cache accordingly.

**Brown-Bag-session similar issue**

13 Apr 2017 session at https://confluence.sea.corp.expecn.com/display/CS/Content+System+Brown+Bag+Sessions

Video: https://bluejeans.com/s/WNziC

Code:https://ewegithub.sb.karmalab.net/acaron/confluent-example

## Console tools
Kafka console tools can be found at Kafka source folder inside “bin” for Linux and “bin/windows” for Windows. Besides creating the starting services, they are particularly useful to replace the actual producer and consumers.
 
The page below explains the available commands in Kafka console tools:

(http://www.javashuo.com/content/p-6671249.html) (I could not a better source)
 
## Kafka Streams
This site displays the official Kafka Streams Documentation: https://kafka.apache.org/documentation/streams
 
It explains the Streams topology, operations and different level processors.
 
One the core operation is join. You can find extra details in the follow links:

http://docs.confluent.io/current/streams/developer-guide.html#joining-streams

https://kafka.apache.org/documentation/streams#streams_dsl_joins
 
A nice groups of Java and Scala examples can be found at 
https://github.com/confluentinc/examples/tree/master/kafka-streams
 
Some best practices details can be found at 
https://community.hortonworks.com/articles/49789/kafka-best-practices.html
 
## Useful commands
Below a list of useful commands for Kafka console tools.
 
### Start zookeeper

```
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```
 
By default, Zookeeper starts at port 2181.
 
### Start kafka server/broker
```
bin\windows\kafka-server-start.bat config\server.properties
```
 
By default, Kafka Broker starts at por 9092.
 
### Create a topic (“test” topic in the example)
```
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```

### Start a producer and send a message
The example commands allows to start a producer that connects to the Kafka broker in the default port and waits for messages that will be included in the “test” topic. 
#### Null keys
```
 bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test
 ```
 
#### Keys and values
```
 bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test --property "parse.key=true" --property key.separator="-"
 ```
 
The separator symbol can be defined: in this case, key and value are separated by the symbol “-”.

### Start a consumer
The example commands allows to start a consumer that connects to the Kafka broker in the default port and reads messages that from the “test” topic.
#### Only values
```
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning
 ```
 
#### Keys and values
```
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --property print.key=true --property key.separator="-" --from-beginning
 ```
The separator symbol can be defined: in this case, key and value are separated by the symbol “-”.
 
## Confluent in Windows
[Confluent](https://www.confluent.io) provides Cloud Kafka and Avro support. Anyway, bat files do not work in the official release. You can find an improvement release here (but you can’t start the schema-registry): https://github.com/renukaradhya/confluentplatform
 
# POC code
Two alternative stream processors are included in the code.
 
## First version
The code includes a simple `StreamProcessor` that joins strings from three topics (“title”, “test” and “location”) and concatenates those strings when one of them changes.
 
### How to see it working?

Steps:
* Start Zookeeper and Kafka Broker.
* Using the topic script, create topics for “title”, “test” and “location”.
* Start a producer for each topic with an individual command line for each one:

 ```
 bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic title --property "parse.key=true" --property key.separator="-"
 ```
 ```
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test --property "parse.key=true" --property key.separator="-"
 ```
 ```
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic location --property "parse.key=true" --property key.separator="-"
 ```
 
 After each initiation, the producer command line will wait for messages. Example:
 
 ```
 bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic title --property "parse.key=true" --property key.separator="-"
hotel3-Grand Hotel
hotel1-Hotel Star
```
 
In the previous example, two messages were sent (message 1: hotel3-Grand Hotel; message 2: hotel1-Hotel Star).

* Run the StreamProcessor via  
`com.expedia.content.systems.property.merge.StreamProcessor`
 
* This processor includes a print stage, but you can also create a consumer to get the merging result. 
    1. Create a topic for "final-results"
    2. Create a consumer via command line using: `bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic final-results --property print.key=true --property key.separator="-" --from-beginning`
 
## Second version
A more complex example can be found with the name `StreamProcessorForIncomingStringJson`. As the previous processor, it waits for changes in three topics but works with json values. When a change appears in one of these topics, the updated key-value pair will trigger the processor activity. The key will be used to find the pairing key-value pair at the other two topics (they are KTable so they work as changelogs) and the updated value will be merge with the values in the not-updated topics. The result will be streamed to a new topic.
 
### How to see it working?

Steps:
* Start Zookeeper and Kafka Broker.
* Using the topic script, create topics for “incoming-json”, “incoming-json2” and “incoming-json3”.
* Start a producer for each topic with an individual command line for each one:
 
 ```
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic incoming-json --property "parse.key=true" --property key.separator="-"
 ```
 ```
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic incoming-json2 --property "parse.key=true" --property key.separator="-"
 ```
 ```
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic incoming-json3 --property "parse.key=true" --property key.separator="-"
 ```
 
Example messages:
```
hotel1-{“title”: “Grand Hotel”}
 ```
 
* Run the StreamProcessor via `com.expedia.content.systems.property.merge.StreamProcessorForIncomingStringJson`
 
* This processor includes a print stage, but you can also create a consumer to get the merging result.
 
    1. Create a topic for "final-result"
    2. Create a consumer via command line using: `bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic final-result --property print.key=true --property key.separator="-" --from-beginning`

