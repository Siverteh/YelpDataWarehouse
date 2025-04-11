#!/bin/bash
# run_robust_kafka.sh - Run the robust Kafka consumer & producer

echo "=== RUNNING ROBUST KAFKA SOLUTION ==="

# Set defaults
COUNT=50
INTERVAL=1.0

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --count=*)
            COUNT="${1#*=}"
            ;;
        --interval=*)
            INTERVAL="${1#*=}"
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Run robust Kafka consumer and producer."
            echo
            echo "Options:"
            echo "  --count=N       Number of events to generate (default: 50)"
            echo "  --interval=X    Time interval between events in seconds (default: 1.0)"
            echo "  --help          Display this help message"
            exit 0
            ;;
        *)
            echo "Unknown parameter: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
    shift
done

# Make sure the robust consumer is in the container
sudo docker cp streaming/robust_consumer.py yelp-kafka-streaming:/app/robust_consumer.py
sudo docker-compose exec kafka_streaming chmod +x /app/robust_consumer.py

# Ensure Kafka topics exist
echo "Creating Kafka topics..."
docker exec yelp-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-reviews
docker exec yelp-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-checkins

# List topics to confirm creation
echo "Listing Kafka topics..."
docker exec yelp-kafka kafka-topics --list --bootstrap-server kafka:9092

# Stop any existing consumer
echo "Stopping any existing consumer..."
sudo docker-compose exec kafka_streaming ps aux | grep "python /app/robust_consumer.py" | grep -v grep | awk '{print $2}' | xargs -r sudo docker-compose exec kafka_streaming kill

# Start the robust consumer in the background
echo "Starting robust consumer in background..."
sudo docker-compose exec -d kafka_streaming python /app/robust_consumer.py

# Wait for consumer to initialize
echo "Waiting for consumer to initialize..."
sleep 3

# Start producer to generate synthetic data
echo "Starting synthetic data generation with producer..."
sudo docker-compose exec kafka_streaming python producer.py --interval $INTERVAL --count $COUNT

echo
echo "Synthetic data generation complete."
echo "The robust consumer is running in the background and will continue to process messages."
echo "You should now see database updates when you refresh the page."
echo
echo "To view consumer logs: sudo docker-compose exec kafka_streaming cat /var/log/consumer.log"
echo "To stop the consumer: sudo docker-compose restart kafka_streaming"
