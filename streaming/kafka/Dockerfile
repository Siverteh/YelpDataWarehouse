FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python pymysql pymongo neo4j requests

# Copy consumer code
COPY kafka_consumer.py .

# Run the Kafka consumer
CMD ["python", "-u", "kafka_consumer.py"]