from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from cassandra.cluster import Cluster
import sys
import traceback

# Define schema
try:
    crypto_schema = StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("market_cap", FloatType(), True),
        StructField("price_change_24h", FloatType(), True)
    ])
    print("Schema defined successfully.")
except Exception as e:
    print("Error defining schema:", e)
    traceback.print_exc(file=sys.stdout)

# Cassandra setup: keyspace and table
try:
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect()
    
    # Create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS crypto 
        WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
    """)
    
    # Create table
    session.execute("""
        CREATE TABLE IF NOT EXISTS crypto.market_data (
            id text ,
            symbol text,
            price double,
            market_cap double,
            price_change_24h double,
            fetched_at timestamp,
            PRIMARY KEY (id, fetched_at)
        );
    """)
    print("Cassandra keyspace and table ensured.")
except Exception as e:
    print("Error setting up Cassandra:", e)
    traceback.print_exc(file=sys.stdout)

# Spark session
try:
    spark = SparkSession.builder \
        .appName("KafkaCryptoConsumer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()
    print("Spark session created successfully.")
except Exception as e:
    print("Error creating Spark session:", e)
    traceback.print_exc(file=sys.stdout)

# Read from Kafka
try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "crypto_data") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    print("Reading from Kafka successful.")
except Exception as e:
    print("Error reading from Kafka:", e)
    traceback.print_exc(file=sys.stdout)

# Parse JSON value
try:
    crypto_df = df.filter(col("topic") == "crypto_data") \
        .select(from_json(col("value").cast("string"), crypto_schema).alias("data")) \
        .select("data.*") \
        .withColumn("fetched_at", current_timestamp())
    print("Parsed Kafka JSON successfully.")
except Exception as e:
    print("Error parsing JSON value:", e)
    traceback.print_exc(file=sys.stdout)

# Write to Cassandra
try:
    query = crypto_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "crypto") \
        .option("table", "market_data") \
        .option("checkpointLocation", "/tmp/checkpoint_crypto") \
        .start()
    print("Writing to Cassandra started successfully.")
    query.awaitTermination()
except Exception as e:
    print("Error writing to Cassandra or during streaming:", e)
    traceback.print_exc(file=sys.stdout)
