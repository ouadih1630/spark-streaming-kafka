# Import necessary libraries from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define Kafka topic and bootstrap server information
KAFKA_TOPIC_NAME_CONS = "first_topic"            # Kafka topic to consume data from
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"  # Kafka bootstrap servers

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")
    
    # Create a SparkSession
    spark = SparkSession \
       .builder \
       .appName("PySpark Structured Streaming with Kafka Demo") \
       .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads data from the specified Kafka topic
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json")
    
    # Define the schema for the JSON data
    jsonSchema = StructType([
        StructField("location", StructType([
                StructField("name", StringType(), True),
                StructField("region", StringType(), True),
                StructField("country", StringType(), True),
                StructField("lat", DoubleType(), True),
                StructField("localtime", StringType(), True),
        ]), True),
        StructField("current", StructType([
                StructField("last_updated_epoch", IntegerType(), True),
                StructField("last_updated", StringType(), True),
                StructField("temp_c", DoubleType(), True),
                StructField("temp_f", DoubleType(), True),
                StructField("wind_mph", DoubleType(), True),
                StructField("wind_kph", DoubleType(), True),
                StructField("wind_degree", IntegerType(), True),
                StructField("wind_dir", StringType(), True),
        ]), True),
    ])
    
    # Convert the value column (JSON data) to a structured DataFrame
    processed_df = kafka_df.select(from_json(col("json"), jsonSchema).alias("data")).select("data.*")
    
    # Select specific fields from the structured DataFrame
    processed_df_1 = processed_df.select("location").select("location.name", "location.country")   
    
    # Write the processed data to the console (for demonstration purposes)
    query = processed_df_1.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    
    # Await termination of the streaming query
    query.awaitTermination()
