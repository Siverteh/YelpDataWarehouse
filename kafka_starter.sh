#!/bin/bash

# Kafka Streaming Starter Script for headless environments
# This script starts both the producer and consumer for Kafka streaming

# Create Kafka topics if they don't exist
echo "Creating Kafka topics..."
sudo docker exec -it yelp-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic yelp-reviews
sudo docker exec -it yelp-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic yelp-checkins

# List topics to confirm creation
echo "Listing Kafka topics..."
sudo docker exec -it yelp-kafka kafka-topics --list --bootstrap-server localhost:9092

# Start producer and consumer in background
echo "Starting Kafka producer and consumer in background..."

# Start the producer in background with output to producer.log
echo "Starting producer (output in producer.log)..."
nohup python kafka_producer.py 2 1000 > producer.log 2>&1 &
PRODUCER_PID=$!
echo "Producer started with PID: $PRODUCER_PID"

# Wait a moment to ensure producer is running
sleep 2

# Start the consumer in background with output to consumer.log
echo "Starting consumer (output in consumer.log)..."
nohup python3 kafka_consumer.py > consumer.log 2>&1 &
CONSUMER_PID=$!
echo "Consumer started with PID: $CONSUMER_PID"

echo "Kafka streaming services started in the background."
echo "To view producer logs: tail -f producer.log"
echo "To view consumer logs: tail -f consumer.log"
echo "To stop the services, run: kill $PRODUCER_PID $CONSUMER_PID"
echo "Or find and kill the processes with: ps aux | grep 'kafka_[pc]'"

# Write the PIDs to a file for easy reference
echo "$PRODUCER_PID $CONSUMER_PID" > kafka_pids.txt
echo "Process IDs saved to kafka_pids.txt"