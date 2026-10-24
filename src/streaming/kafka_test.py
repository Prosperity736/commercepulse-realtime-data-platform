from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CommercePulseKafkaTest")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark started successfully.")
print("Attempting to connect to Kafka...")

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "olist-orders")
    .option("startingOffsets", "earliest")
    .load()
)

print("Successfully connected to Kafka!")
print("Number of Kafka records:", df.count())

spark.stop()