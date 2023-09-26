// Import necessary Spark libraries for Structured Streaming
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Define an object named "Test" for the application
object Test {
  // Define Kafka topic and bootstrap server information as constants
  val KAFKA_TOPIC_NAME_CONS = "first_topic"             // Kafka topic to consume data from
  val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"   // Kafka bootstrap servers

  // Main method for the Spark Structured Streaming application
  def main(args: Array[String]): Unit = {
    // Print a startup message
    println("Spark Structured Streaming with Kafka Demo Application Started")

    // Create a SparkSession
    val spark = SparkSession
      .builder()
      .appName("Test")
      .getOrCreate()

    // Import Spark implicits for DataFrame operations
    import spark.implicits._

    // Set the log level to ERROR to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")

    // Construct a streaming DataFrame that reads data from the specified Kafka topic
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "earliest")
      .load()

    // Select the "value" column from Kafka, treating it as a string (JSON data)
    val structuredKafkaDF = kafkaDF
      .selectExpr("CAST(value AS STRING) as json")

    // Define a schema for parsing the JSON data
    val jsonSchema = StructType(
      Array(
        StructField("location", StructType(
          Array(
            StructField("name", StringType, nullable = true),
            StructField("country", StringType, nullable = true)
          )
        )),
        StructField("current", StructType(
          Array(
            StructField("last_updated_epoch", IntegerType, nullable = true),
            StructField("last_updated", StringType, nullable = true)
          )
        ))
      )
    )

    // Parse the JSON data using the defined schema
    val parsedDF = structuredKafkaDF.select(from_json(col("json"), jsonSchema).alias("data")).select("data.*")

    // Select specific fields from the parsed DataFrame
    val parsedDF_1 = parsedDF.select("location.name", "location.country")

    // Define the streaming query with output to the console
    val query = parsedDF_1.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))  // Set a processing time trigger
      .start()

    // Await termination of the streaming query
    query.awaitTermination()
  }
}
