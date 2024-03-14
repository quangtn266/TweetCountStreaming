package com.quangtn.github;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.node.ObjectNode;
import scala.Tuple2;
import scala.Tuple3;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StreamingETL {

    public static void main(String[] args) throws Exception {
        // parse arguments
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        // create streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable event time processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // enable fault-tolerance
        env.enableCheckpointing(1000);

        // enable restarts
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50,
                500L));

        env.setStateBackend(new FsStateBackend("file:////Users/quangtn/Desktop/01_work/" +
                "01_job/03_Flink/tweetwordcount/state_backend"));

        // run each operator separately
        env.disableOperatorChaining();

        // get data from kafka
        Properties kParams = params.getProperties();
        kParams.setProperty("group.id", UUID.randomUUID().toString());
        DataStream<ObjectNode> inputStream = env.addSource(new FlinkKafkaConsumer09<>
                (params.getRequired("topic"), new JSONDeserializationSchema(), kParams))
                .name("Kafka Source")
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.minutes(1L)) {
                    @Override
                    public long extractTimestamp(ObjectNode jsonNodes) {
                        return jsonNodes.get("timestamp_ms").asLong();
                    }
                }).name("Timestamp extractor");

        // filtered out records without lang field
        DataStream<ObjectNode> tweetsWithLang = inputStream.filter(jsonNodes -> jsonNodes.has("user")
                && jsonNodes.get("user").has("lang")).name("Filter records without 'lang' field");

        // select only lang = "en" tweets
        DataStream<ObjectNode> englishTweets = tweetsWithLang.filter(jsonNodes -> jsonNodes.get("user")
                .get("lang").asText().equals("en")).name("Select 'lang'=en tweets");

        // write to file system
        RollingSink<ObjectNode> rollingSink = new RollingSink<>(params
          .get("sinkPath","/Users/quangtn/Desktop/01_work/01_job/03_Flink/tweetwordcount/rolling_sink"));

        // do a bucket for each minute
        rollingSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd-HH-mm"));
        englishTweets.addSink(rollingSink).name("Rolling FileSystem Sink");

        // build aggregate (count per language) using window (10 seconds tumbling):
        DataStream<Tuple3<Long, String, Long>> languageCounts = tweetsWithLang.keyBy(jsonNode -> jsonNode
                .get("user").get("lang").asText()).timeWindow(Time.seconds(10))
                .apply(new Tuple3<>(0L, "", 0L), new JsonFoldCounter(), new CountEmitter())
                .name("Count per Language (10 seconds tumbling)");

        // write window aggregate to ElasticSearch
        List<InetSocketAddress> transportNodes = ImmutableList.of(new InetSocketAddress(InetAddress.getByName("localhost"),9300));
        ElasticsearchSink<Tuple3<Long, String, Long>> elasticsearchSink = new ElasticsearchSink<>(params.toMap(), transportNodes, new ESRequest());

        languageCounts.addSink(elasticsearchSink).name("ElasticSearch2 Sink");

        // word-count on the tweet stream
        DataStream<Tuple2<Date, List<Tuple2<String, Long>>>> topWordCount = tweetsWithLang
                // get text from tweets
                .map(tweet -> tweet.get("tweet").asText()).name("Get text from Tweets")
                // split text into (word, 1) tuples
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String [] splits = s.split(" ");
                        for (String sp: splits) {
                            collector.collect(new Tuple2<>(sp, 1L));
                        }
                    }
                }).name("Tokenize words")
                // group by word
                .keyBy(0)
                // build 1 min window, compute every 10 seconds --> count word frequency
                .timeWindow(Time.minutes(1L), Time.seconds(10L)).apply(new WordCountingWindow())
                .name("Count word frequency (1 min, 10 sec sliding window)")
                // build top n every 10 seconds
                .timeWindowAll(Time.seconds(10L)).apply(new TopWords(10)).name("TopN window (10s)");

        // write top Ns to Kafka topic
        topWordCount.addSink(new FlinkKafkaProducer09<>(params.getRequired("wc-topic"),
                new ListSerSchema(), params.getProperties())).name("Write topN to Kafka");

        env.execute("Streaming ETL");

    }

    private static class JsonFoldCounter implements FoldFunction<ObjectNode, Tuple3<Long, String, Long>> {
        @Override
        public Tuple3<Long, String, Long> fold(Tuple3<Long, String, Long> current, ObjectNode o) throws Exception {
            current.f0++;
            return current;
        }
    }

    private static class CountEmitter implements WindowFunction<Tuple3<Long, String, Long>, Tuple3>
}
