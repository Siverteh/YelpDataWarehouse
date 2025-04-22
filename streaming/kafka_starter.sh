#!/bin/bash
# kafka_starter.sh - Enhanced script to run Kafka consumer and producer with correct paths

echo "=== RUNNING ENHANCED KAFKA PIPELINE FOR YELP DATA WAREHOUSE ==="

# Set defaults
COUNT=100
INTERVAL=0.5

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
            echo "Run Kafka consumer and producer for Yelp Data Warehouse."
            echo
            echo "Options:"
            echo "  --count=N       Number of events to generate (default: 100)"
            echo "  --interval=X    Time interval between events in seconds (default: 0.5)"
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

# Copy the enhanced scripts to the container - using correct paths
echo "Setting up enhanced scripts..."
# Using the files from the streaming directory
sudo docker cp ./streaming/consumer.py yelp-kafka-streaming:/app/consumer.py
sudo docker cp ./streaming/producer.py yelp-kafka-streaming:/app/producer.py
sudo docker-compose exec kafka_streaming chmod +x /app/consumer.py
sudo docker-compose exec kafka_streaming chmod +x /app/producer.py

# Ensure Kafka topics exist (including new topics for businesses and users)
echo "Creating Kafka topics..."
sudo docker exec yelp-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-reviews
sudo docker exec yelp-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-checkins
sudo docker exec yelp-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-businesses
sudo docker exec yelp-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic yelp-users

# List topics to confirm creation
echo "Listing Kafka topics..."
sudo docker exec yelp-kafka kafka-topics --list --bootstrap-server kafka:9092

# Stop any existing consumer processes
echo "Stopping any existing consumer processes..."
sudo docker-compose exec kafka_streaming pkill -f "python /app/consumer.py" || true

# Start the enhanced consumer in the background
echo "Starting enhanced consumer in background..."
sudo docker-compose exec -d kafka_streaming python /app/consumer.py

# Wait for consumer to initialize
echo "Waiting for consumer to initialize..."
sleep 5

# Start producer to generate synthetic data
echo "Starting synthetic data generation with producer ($COUNT events, $INTERVAL second interval)..."
echo "This will create reviews, checkins, businesses, and users!"
sudo docker-compose exec kafka_streaming python /app/producer.py --interval $INTERVAL --count $COUNT

echo
echo "Synthetic data generation complete."
echo "The consumer is running in the background and will continue to process messages."
echo
echo "You should now see database updates when you refresh the web dashboard."
echo "New businesses and users were created in addition to reviews and checkins."
echo
echo "To check if consumer is running: sudo docker-compose exec kafka_streaming ps aux | grep consumer"
echo "To see database statistics:"
echo "  MySQL: sudo docker-compose exec mysql mysql -u root -puser yelp_dw -e 'SELECT COUNT(*) FROM dim_business; SELECT COUNT(*) FROM dim_user; SELECT COUNT(*) FROM fact_review;'"
echo "  MongoDB: sudo docker-compose exec mongodb mongo -u user -p user --authenticationDatabase admin yelp_db --eval 'db.businesses.count(); db.users.count(); db.reviews.count();'"
echo "To stop the consumer: sudo docker-compose exec kafka_streaming pkill -f consumer.py"
echo "To restart the Kafka service: sudo docker-compose restart kafka_streaming"