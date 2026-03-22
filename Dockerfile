# Use Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Expose ports if needed (Spark UI, etc.)
EXPOSE 4040

# Default command: run Twitch producer
CMD ["python", "ingestion/producers/twitch_producer.py"]