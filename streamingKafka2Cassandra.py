from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, TimestampType
from pyspark.sql.functions import from_json, col

# Define the Kafka topic to subscribe to
kafka_bootstrap_servers = "kafka:9092"
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
spark = SparkSession.builder\
    .config("spark.driver.host", "localhost")\
    .config("spark.cassandra.connection.host", "cassandra")\
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .appName("SparkStructuredStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize the Kafka message value as JSON and select the status column
df1 = df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), orderSchema).alias("data")).select("data.*")
# status_column = df1.select("status")

# # Create a streaming DataFrame with a watermark to track the latest status count
# status_counts = df1 \
#     .groupBy("status") \
#     .count()


# Function to write data to Cassandra


def writeToCassandra(writeDF, epoch_id):
    try:
        writeDF.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table="order_data", keyspace="ecommerce") \
            .save()

    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")


# Write data to Cassandra using foreachBatch
query1 = df1.writeStream \
    .foreachBatch(writeToCassandra) \
    .outputMode("update") \
    .start().awaitTermination()
