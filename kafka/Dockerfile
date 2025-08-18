FROM python:3.12-slim

WORKDIR /app

# Copy current folder contents into /app
COPY . /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Copy + make startup script executable
COPY kafka_startup.sh /app/kafka_startup.sh
RUN chmod +x /app/kafka_startup.sh

# Run the startup script
ENTRYPOINT ["/app/kafka_startup.sh"]
