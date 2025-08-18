from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("KafkaWeatherConsumer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_local:9092") \
    .option("subscribe", "weather_data") \
    .option("startingOffsets", "latest") \
    .load()

# Just cast value as string
df.selectExpr("CAST(value AS STRING) as value") \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .start() \
  .awaitTermination()
