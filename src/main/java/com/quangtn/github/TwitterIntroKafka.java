package com.quangtn.github;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

public class TwitterIntroKafka {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> twitterStreamString = env.addSource(new TwitterSource(params.getProperties()));
        DataStream<String> filteredStream  =twitterStreamString.flatMap(new ParseJson());
        filteredStream.flatMap(new ThroughputLogger(5000L)).setParallelism(1);

        filteredStream.addSink(new FlinkKafkaProducer09<>("twitter", new SimpleStringSchema(),
                params.getProperties()));

        // execute program
        env.execute("Ingest data from twitter to Kafka");
    }

    public static class ParseJson implements FlatMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if(jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            // boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang")
            // && jsonNode.get("user").get("lang").getValueAsText().equals("en");
            boolean hasText = jsonNode.has("text");
            if(hasText) {
                out.collect(value);
            }
        }
    }
}
