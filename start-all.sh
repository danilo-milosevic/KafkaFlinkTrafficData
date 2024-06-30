#!/bin/bash

# Start Zookeeper
osascript -e 'tell application "Terminal" to do script "zookeeper-server-start /opt/homebrew/Cellar/kafka/3.7.0/config/zookeeper.properties"'

# Wait for 10 seconds
sleep 15

# Start Kafka
osascript -e 'tell application "Terminal" to do script "kafka-server-start /opt/homebrew/Cellar/kafka/3.7.0/config/server.properties"'
