#!/bin/bash

# For single-host environments

# Start the console consumer, receive all messages from the beginning of the topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic weblogs --from-beginning

# Re-start the console consumer, receive only new messages
kafka-console-consumer --bootstrap-server localhost:9092 --topic weblogs  