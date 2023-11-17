from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType
from pyspark.sql.functions import from_json, col

# Define the Kafka topic to subscribe to
kafka_topic = "sales"

# Define the schema for your order data
orderSchema = StructType([
    StructField("item_id", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("created_at", StringType(), False),
    StructField("price", FloatType(), False),
    StructField("qty_ordered", IntegerType(), False),
    StructField("grand_total", FloatType(), False)
])

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .config("spark.driver.host", "localhost")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# Define the Kafka source for Structured Streaming
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()
df.printSchema()
# Deserialize the Kafka message value as JSON and select fields
df1 = df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), orderSchema).alias("data")).select("data.*")
status_column = df1.select("status")

# Create a streaming DataFrame with a watermark to track the latest status count
status_counts = df1 \
    .groupBy("status") \
    .count()

# Write data to the console
query = status_counts.writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()\
    .awaitTermination()
