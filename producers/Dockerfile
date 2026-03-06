# Base image — lightweight Python 3.11
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install the kafka-python library
# This is the only dependency the producers need
RUN pip install --no-cache-dir kafka-python==2.0.2

# Copy both producer scripts into the container
COPY bgp_producer.py .
COPY dns_producer.py .

# Default command (overridden per service in docker-compose.yml)
CMD ["python", "bgp_producer.py"]
