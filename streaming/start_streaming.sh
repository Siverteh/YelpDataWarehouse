#!/bin/bash
# start_streaming.sh - Start streaming real Yelp data after initialization

# Default values
COUNT=100
INTERVAL=1.0
MODE="mixed"

# Function to display usage information
function show_help {
    echo "Usage: $0 [OPTIONS]"
    echo "Start streaming real Yelp data through Kafka after initialization."
    echo
    echo "Options:"
    echo "  --count=N       Number of events to stream (default: 100)"
    echo "  --interval=X    Time interval between events in seconds (default: 1.0)"
    echo "  --mode=TYPE     Type of data to stream (reviews, checkins, mixed) (default: mixed)"
    echo "  --infinite      Stream indefinitely (overrides --count)"
    echo "  --help          Display this help message"
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --count=*)
            COUNT="${1#*=}"
            ;;
        --interval=*)
            INTERVAL="${1#*=}"
            ;;
        --mode=*)
            MODE="${1#*=}"
            ;;
        --infinite)
            COUNT=""
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown parameter: $1"
            show_help
            exit 1
            ;;
    esac
    shift
done

# Get the root directory (where docker-compose.yml is located)
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

# Check if docker is running
if ! command -v docker &>/dev/null; then
    echo "Error: Docker is not installed or not in your PATH"
    exit 1
fi

# Check if the Docker containers are running
if ! docker ps | grep -q "yelp-mysql"; then
    echo "Error: Yelp Data Warehouse containers are not running."
    echo "Please start the containers first with: sudo docker-compose up -d"
    exit 1
fi

echo "Starting real-time data streaming..."
echo "Mode: $MODE"
if [ -n "$COUNT" ]; then
    echo "Count: $COUNT events"
else
    echo "Count: Infinite streaming"
fi
echo "Interval: $INTERVAL seconds"
echo

# Start the streaming service with the kafka_streamer profile
CMD="--mode=$MODE --interval=$INTERVAL"
if [ -n "$COUNT" ]; then
    CMD="$CMD --count=$COUNT"
fi

# Change to the root directory to run docker-compose
cd "$ROOT_DIR"

echo "Starting the Kafka Streamer service with Docker Compose..."
sudo docker-compose --profile streamer up -d kafka_streamer

# Run the streamer command in the container
echo "Running the streamer with options: $CMD"
sudo docker exec yelp-kafka-streamer python dataset_streamer.py $CMD

echo
echo "Streaming process has started."
echo "To view logs: sudo docker logs -f yelp-kafka-streamer"
echo "To stop: sudo docker-compose stop kafka_streamer"