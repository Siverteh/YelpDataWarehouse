FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy initialization script
COPY init.py .

# Run initialization script when container starts
CMD ["python", "init.py"]