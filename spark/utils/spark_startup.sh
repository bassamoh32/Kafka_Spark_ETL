#!/bin/bash

# Install dependencies
cd utils
pip install -r requirements.txt

# Start Spark consumer
spark-submit --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  spark_consumer.py
