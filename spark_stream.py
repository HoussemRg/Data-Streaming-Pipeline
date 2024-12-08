from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Define the schema for incoming Kafka messages
schema = StructType([
    StructField("id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("post_code", StringType(), True),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("registered_date", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("picture", StringType(), True)
])

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("KafkaToCassandra") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    # Read data from Kafka topic
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "users_created_new_topic") \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract and parse the value field from Kafka messages
    value_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Write the parsed data to Cassandra
    query = value_df.writeStream \
        .outputMode("append") \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .option("keyspace", "spark_streams") \
        .option("table", "created_users") \
        .start()

    # Await termination of the streaming query
    query.awaitTermination()