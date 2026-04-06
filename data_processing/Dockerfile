# Use a slim Python image
FROM python:3.11-slim

# Install system dependencies for confluent-kafka
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    gcc \
    && rm -rf /var/lib/apt/lib/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY ingestor.py .

# Run the script when the container starts
CMD ["python", "ingestor.py"]