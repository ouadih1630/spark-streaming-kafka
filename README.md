This project involves setting up a Kafka producer to fetch weather data for a list of cities from an API and transmit it as JSON messages to the 'first_topic' Kafka topic. The PySpark script establishes a structured streaming job to consume JSON data from Kafka, while the Scala code configures a Spark Structured Streaming job for the same purpose."
#run pyspark 
```
bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1 pyspark.py
```
