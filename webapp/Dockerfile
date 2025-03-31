FROM python:3.10-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies with specific versions
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Explicitly expose the port
EXPOSE 8080

# Print Python package versions for debugging
RUN pip freeze

# Start the Flask application with a delay to wait for databases
CMD ["sh", "-c", "sleep 30 && python app.py"]