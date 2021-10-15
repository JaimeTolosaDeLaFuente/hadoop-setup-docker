#!/bin/bash

# For single-host environments

# Start console producer
kafka-console-producer   --broker-list localhost:9092 --topic weblogs

