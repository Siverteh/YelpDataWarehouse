#!/bin/bash

# Kafka Streaming Starter Script for Yelp Data Warehouse
# This script creates Kafka topics and starts the consumer

# Create Kafka topics if they don't exist
echo "Creating Kafka topics..."
sudo docker exec yelp-kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-reviews
sudo docker exec yelp-kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-checkins

# List topics to confirm creation
echo "Listing Kafka topics..."
sudo docker exec yelp-kafka kafka-topics --list --bootstrap-server kafka:9092

# Start consumer in the background
echo "Starting Kafka consumer..."
sudo docker-compose up -d kafka_consumer

# Check if consumer is running
if sudo docker ps | grep -q "yelp-kafka-consumer"; then
    echo "Kafka consumer started successfully."
    echo "To view logs: sudo docker logs -f yelp-kafka-consumer"
else
    echo "Error: Failed to start Kafka consumer."
    echo "Check logs with: sudo docker-compose logs kafka_consumer"
fi

echo
echo "Kafka infrastructure is now ready."
echo "To start streaming real data, run: ./start_streaming.sh"
echo "Examples:"
echo "  ./start_streaming.sh --count=500 --interval=0.5"
echo "  ./start_streaming.sh --infinite --mode=mixed"