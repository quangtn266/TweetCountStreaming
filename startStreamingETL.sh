#!/bin/bash

# clean up rolling sink
rm -r rolling_sink/*

/Users/quangtn/Desktop/01_work/01_job/03_Flink/flink-1.18.1/bin/start-cluster.sh

/Users/quangtn/Desktop/01_work/01_job/03_Flink/flink-1.18.1/bin/flink run \
/Users/quangtn/Desktop/01_work/01_job/03_Flink/tweetwordcount/target/original-tweetwordcount-1.0-SNAPSHOT.jar \
/Users/quangtn/Desktop/01_work/01_job/03_Flink/tweetwordcount/etl.properties