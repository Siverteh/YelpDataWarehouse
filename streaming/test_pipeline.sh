#!/bin/bash
# test_pipeline.sh - Test the entire Kafka streaming pipeline

echo "=== TESTING KAFKA STREAMING PIPELINE ==="

# 1. Check Kafka topics exist
echo "Step 1: Checking Kafka topics..."
docker exec yelp-kafka kafka-topics --list --bootstrap-server kafka:9092

# 2. Check if consumer is running
echo "Step 2: Checking for running consumer process..."
sudo docker-compose exec kafka_streaming ps -ef | grep consumer.py

# 3. Test webapp connectivity from kafka_streaming
echo "Step 3: Testing connection to webapp API..."
sudo docker-compose exec kafka_streaming curl -s http://webapp:8080/api/test || echo "Failed to connect to webapp"

# 4. Start consumer in verbose mode
echo "Step 4: Starting Kafka consumer with debug output..."
sudo docker-compose exec -d kafka_streaming sh -c "python consumer.py > /tmp/consumer_debug.log 2>&1"

# 5. Send a test message
echo "Step 5: Sending one test message..."
sudo docker-compose exec kafka_streaming python -c "
from kafka import KafkaProducer
import json
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)
test_msg = {
    'review_id': 'TEST_MESSAGE',
    'business_id': 'test_business',
    'user_id': 'test_user',
    'stars': 5,
    'date': '2025-04-07T12:00:00',
    'text': 'Test message',
    'source': 'test'
}
producer.send('yelp-reviews', key='test_business', value=test_msg)
producer.flush()
print('Test message sent to Kafka')
"

# 6. Check consumer logs
echo "Step 6: Checking consumer log output..."
sleep 5
sudo docker-compose exec kafka_streaming cat /tmp/consumer_debug.log

# 7. Check webapp logs 
echo "Step 7: Checking webapp logs for broadcast events..."
sudo docker logs --tail=20 yelp-webapp

echo "=== PIPELINE TEST COMPLETE ==="