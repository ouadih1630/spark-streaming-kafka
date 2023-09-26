This project involves setting up a Kafka producer to fetch weather data for a list of cities from an API and transmit it as JSON messages to the 'first_topic' Kafka topic. The PySpark script establishes a structured streaming job to consume JSON data from Kafka, while the Scala code configures a Spark Structured Streaming job for the same purpose."
# Run pyspark 
```
bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1 pyspark.py
```
# Run spark with scala 
```
run sbt package 
bin/spark-submit   --class Test  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1 ./target/scala-2.13/test_2.13-1.0.jar
```
