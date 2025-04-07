FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python pymysql pymongo requests

# Copy streamer code
COPY kafka/dataset_streamer.py /app/

# Provide a default command
CMD ["python", "-u", "dataset_streamer.py", "--mode=mixed", "--count=100", "--interval=1.0"]