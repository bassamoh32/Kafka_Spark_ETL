FROM python:3.10-slim

# Install Java 21 + utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jdk-headless \
        wget \
        curl \
        procps \
        ca-certificates \
        netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for OpenJDK 21
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app
COPY . /app/

RUN pip install --no-cache-dir -r requirements.txt

# Spark + Kafka connector
ENV PYTHONPATH=/app
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"

# Copy startup script
COPY start_spark_consumer.sh /app/start_spark_consumer.sh
RUN chmod +x /app/start_spark_consumer.sh

# Use the script as entrypoint
ENTRYPOINT ["/app/start_spark_consumer.sh"]