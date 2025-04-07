#!/bin/bash
# kafka-stream.sh - Unified script for Kafka topic creation and data streaming

# Set defaults
MODE="mixed"
COUNT=100
INTERVAL=1.0

# Function to display usage information
function show_help {
    echo "Usage: $0 [OPTIONS]"
    echo "Manage Kafka topics and data streaming for Yelp Data Warehouse."
    echo
    echo "Options:"
    echo "  --mode=TYPE     Operation mode: consumer, producer, mixed (default: mixed)"
    echo "  --count=N       Number of events to stream (default: 100)"
    echo "  --interval=X    Time interval between events in seconds (default: 1.0)"
    echo "  --help          Display this help message"
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --mode=*)
            MODE="${1#*=}"
            ;;
        --count=*)
            COUNT="${1#*=}"
            ;;
        --interval=*)
            INTERVAL="${1#*=}"
            ;;
        --help)
            show_help
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

echo "Starting Kafka streaming..."
echo "Mode: $MODE"
echo "Count: $COUNT events"
echo "Interval: $INTERVAL seconds"
echo

# Create Kafka topics if they don't exist
echo "Creating Kafka topics..."
docker exec yelp-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-reviews
docker exec yelp-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-checkins

# List topics to confirm creation
echo "Listing Kafka topics..."
docker exec yelp-kafka kafka-topics --list --bootstrap-server kafka:9092

# Run the streaming command based on mode
echo "Starting Kafka streaming in $MODE mode..."
if [ "$MODE" == "consumer" ]; then
    # Run consumer mode (listens for messages)
    docker exec -d yelp-kafka-streaming python streaming.py --consumer
elif [ "$MODE" == "producer" ]; then
    # Run producer mode (generates simulated data)
    docker exec -it yelp-kafka-streaming python streaming.py --producer --count=$COUNT --interval=$INTERVAL
else
    # Run mixed mode (streams real data if available)
    docker exec -it yelp-kafka-streaming python streaming.py --mixed --count=$COUNT --interval=$INTERVAL
fi

echo
echo "Streaming process has started."
echo "To view logs: docker logs -f yelp-kafka-streaming"
echo "To stop: docker-compose stop kafka_streaming"