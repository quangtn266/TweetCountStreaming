#!/usr/bin/env bash


 ~/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic twitter-wordcount --partitions 1 --replication-factor 1
