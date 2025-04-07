FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python pymysql pymongo neo4j requests

# Copy streaming code
COPY *.py ./

# Default command (can be overridden from docker-compose or command line)
CMD ["python", "-u", "streaming.py", "consumer"]