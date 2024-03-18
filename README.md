# TweetCountStreaming
A flink demo is about Tweet Counting (Streaming)

# Prepare for running.

1. Setup Kafka engine
```
 ~/kafka_2.13-3.6.0/bin/kafka-storage.sh random-uuid
````

2. Configure for Kafka server
```
~/kafka_2.13-3.6.0/bin/kafka-storage.sh format -t <above uuid result> ~/kafka_2.13-3.6.0/config/kraft/server.properties
```

3. Start Kafka server
```
~/kafka_2.13-3.6.0/bin/kafka-server-start.sh ~/kafka_2.13-3.6.0/config/kraft/server.properties
```

4. Create a topic.
```
bash createtopic.sh
```

5. Building .jar package
```
mvn clean package
```

6. Start flink engine and running flink
```
bash startStreamingETL.sh
```
