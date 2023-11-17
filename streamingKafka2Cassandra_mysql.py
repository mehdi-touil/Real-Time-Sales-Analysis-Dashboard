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
    StructField("grand_total", FloatType(), False),
])

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("SparkStructuredStreaming") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars", "mysql-connector-java-8.0.13.jar,spark-cassandra-connector_2.12-3.1.0.jar") \
    .config("spark.driver.extraClassPath", "mysql-connector-java-8.0.13.jar,spark-cassandra-connector_2.12-3.1.0.jar") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define the Kafka source for Structured Streaming
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize the Kafka message value as JSON
df1 = df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), orderSchema).alias("data")).select("data.*")

# Create a streaming DataFrame with a watermark to track the latest status count
status_counts = df1 \
    .groupBy("status") \
    .count()

# Write to MySQL
query_mysql = status_counts \
    .writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: df.write
                  .format("jdbc")
                  .option("url", "jdbc:mysql://172.18.0.6:3306/mydb")
                  .option("dbtable", "status_counts_table")
                  .option("user", "mehdi")
                  .option("password", "mehdi")
                  .mode("overwrite")
                  .save()) \
    .start()

# Write to Cassandra


def writeToCassandra(writeDF, epoch_id):
    try:
        writeDF.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table="order_data", keyspace="ecommerce") \
            .save()

    except Exception as e:
        print(f"Error writing to Cassandra: {str(e)}")


query_cassandra = df1.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(writeToCassandra) \
    .outputMode("update") \
    .start()

# Await termination of both queries
query_mysql.awaitTermination()
query_cassandra.awaitTermination()

# Stop the Spark session
spark.stop()
