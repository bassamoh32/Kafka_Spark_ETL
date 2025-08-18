#!/bin/bash

# Wait for Spark Master
until nc -z spark-master 7077; do
    echo "Waiting for Spark Master to start ..."
    sleep 3
done

# Wait for Kafka
until nc -z kafka_local 9092; do
    echo "Waiting for Kafka broker to start ..."
    sleep 3
done

# Start Spark consumer
echo "Starting Spark consumer ..."
exec python spark_consumer.py
