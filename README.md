# End-to-End ETL using Apache Kafka and Apache Spark

## Overview 
This project ingests real-time cryptocurrency market data from the CoinGecko API and processes it through a modern streaming data pipeline. It uses Kafka as a message broker, Spark Structured Streaming for data transformation and schema enforcement, and Cassandra for scalable storage of historical market data. The architecture is designed for fault tolerance and near real-time analytics, making it suitable for dashboards, monitoring systems, and machine learning use cases. The entire pipeline is containerized and orchestrated with Docker Compose for easy deployment and reproducibility.

## Architecture 
*(You can insert an architecture diagram here for better visualization)*

### Components

- **CoinGecko API** – Source of real-time cryptocurrency market data.  
- **Kafka Producer** – Python script to fetch data from the CoinGecko API and publish it to Kafka.  
- **Kafka** – Message broker to handle streaming crypto data.  
- **Apache Spark** – For stream processing, transformation, and schema enforcement.  
- **Cassandra** – Database to store processed and historical crypto market data.  
- **Docker & Docker Compose** – Containerization and orchestration of Kafka, Spark, and Cassandra services.  
- **Kafdrop** – Kafka monitoring tool for inspecting topics and messages.

## Run Project

After cloning the project, follow these steps to run the pipeline:

1. Start all services using Docker Compose:

```bash
docker-compose up --build
```
Use the Kafdrop UI to verify that the producer is publishing data into the crypto_data topic.

Check that Cassandra is running:

```bash
docker exec -it cassandra cqlsh 
```

2. Run the Spark job script:

```bash
docker exec -it spark-master /bin/bash /utils/spark_startup.sh 
```

3. Verify the data in Cassandra:

```bash
SELECT * FROM crypto.market_data;
```