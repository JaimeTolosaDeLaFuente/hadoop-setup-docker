#!/bin/bash

# For single-host environments

# Create weblogs topic
kafka-topics --create  --zookeeper localhost:2181 --replication-factor 1  --partitions 2  --topic weblogs


# List topics
kafka-topics --list  --zookeeper localhost:2181


# Describe weblogs topic
kafka-topics --describe weblogs  --zookeeper localhost:2181
