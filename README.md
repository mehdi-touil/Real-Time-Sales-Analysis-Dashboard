# Real Time Sales Analysis Dashboard - Streaming Data Pipeline


## Overview

This project is a real-time sales analysis dashboard implemented as a streaming data pipeline. It leverages Apache Spark, Kafka, MySQL, and Cassandra to process and analyze sales data in real-time, with the results visualized through Apache Superset.

## Components

- **Kafka:** Acts as the data source for streaming events.
- **Apache Spark:** Processes and analyzes streaming data.
- **MySQL:** Stores aggregated metrics and results for SQL-style querying.
- **Cassandra:** Stores raw event data or denormalized/aggregated data for fast reads.
- **PowerBI :** Visualization tool for creating a real-time sales dashboard.

## Architecture
![](/images/sales-streaming-architecture.png)

All applications in this project are containerized into **Docker** containers to easily setup the environment for end to end streaming data pipeline.

The pipeline has following layers:
- Data Ingestion Layer
- Message Broker Layer
- Stream Processing Layer
- Serving Database Layer
- Visualization Layer

Let's review how each layer is doing its job.

### Data Ingestion Layer
An application Ecommerce APi  provides a REST layer on top of kafka producer. User make an order in the application which retrieves the data in json format, performs data validation and ingests this data in Kafka broker.

### Message Broker Layer
Messages from FastAPI based Python application are consumed by kafka broker which is located inside the kafka service container. The first `kafka` service launches the kafka instance and creates a broker and create *Sales* topic inside the `kafka` instance. The `zookeeper` service is launched before kafka as it is required for its metadata management.

### Stream Processing Layer
A spark application called `spark-streaming` is submitted to spark cluster manager along with the required jars. This application connects to Kafka broker to retrieve messages from *Sales* topic, transforms them using Spark Structured Streaming and loads them into Cassandra and Mysql tables. The first query transforms data into format accepted by cassandra table and second query aggregates this data to load into mysql.

**Spark Jars:**

Following are the spark jars required for stream processing:
- spark-sql-kafka-0-10_2.12:3.0.0
- com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 
- kafka-clients-3.4.0.jar
- spark-sql-kafka-0-10_2.12-3.3.0.jar
- spark-streaming-kafka-0-10-assembly_2.12-3.3.0.jar
- jsr166e-1.1.0.jar
- mysql-connector-java-8.0.28.jar

The .jar files can easily be downloaded from maven.

### Serving Database Layer
A cassandra database stores and persists raw data and mysql database stores the aggregated data from Spark jobs. The fisrt `cassandra` service is responsible for launching the cassandra instance and `mysql` for MYSQL Database.

### Visualization Layer
The `PowerBI` Software retrieves the data realtime. PowerBI connects to MySQL database and visualizes sales data. The dashboard is refreshed every 10 seconds.


## Dashboard
![](/images/dashboard.png)

