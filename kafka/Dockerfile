FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*
    
# Copy current folder contents into /app
COPY . /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy + make startup script executable
# COPY kafka_startup.sh /app/kafka_startup.sh
RUN chmod +x /app/kafka_startup.sh

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Run the startup script
ENTRYPOINT ["/app/kafka_startup.sh"]
