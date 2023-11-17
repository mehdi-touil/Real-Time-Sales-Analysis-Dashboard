from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the Kafka topic to consume from
kafka_topic = "sales_topic"

# Define the schema for your data
dataSchema = StructType([
    StructField("status", StringType(), False),
    StructField("count", IntegerType(), False)
])

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

# Read messages from Kafka topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", kafka_topic) \
    .load()

# Deserialize the Kafka message value as JSON
df1 = df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), dataSchema).alias("data")).select("data.*")

# Print the streaming results to the console
query = df1 \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the termination of the query
query.awaitTermination()

# Stop the Spark session
spark.stop()
