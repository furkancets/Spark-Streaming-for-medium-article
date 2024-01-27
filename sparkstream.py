from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, split, explode
import logging
import logging.config
import os
logging.config.fileConfig(fname='logutils/logging.conf')
logger = logging.getLogger(__name__)

# Define your schema
schema = StructType([
    StructField("source", StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urlToImage", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("content", StringType(), True),
])
"""
This block defines a schema for the structured streaming data. 
The schema represents the expected structure of the data to be read from the Kafka topic.
"""

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingExample") \
    .master("spark://master:7077") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.logConf", "true") \
    .getOrCreate()
logger.info(f"Spark object crated ...")
"""
Here, a Spark session is initialized with specific configurations such as the application name, master URL, 
executor cores and memory settings, 
and dependencies for Kafka integration.
"""

#spark.sparkContext.setLogLevel("DEBUG")

# Read from Kafka topic
lines = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "master:9092,node1:9092,node2:9092") \
    .option("subscribe", "newsapiproducerv1") \
    .load() 
logger.info(f"Spark streaming reading beginning ...")
"""
This block sets up a structured streaming source from a Kafka topic named "newsapiproducerv1."
"""

logger.info(f"Spark data manupulation is beginning ...")
# Convert Kafka message value to a string
kafka_stream_df = lines.selectExpr("CAST(key as STRING) as key", "CAST(value AS STRING) as value") 
"""
It casts the key and value columns from Kafka messages to strings.
"""

# Use from_json function in select
parsed_df = kafka_stream_df.select("key", "value", from_json(col("value"), schema).alias("data"))
"""
This line applies the from_json function to the "value" column, using the previously defined schema. 
The result is a DataFrame with a new column named "data" containing the parsed JSON.
"""

# Select individual columns from the parsed DataFrame
select_df = parsed_df.select("data.description") 
"""
Here, only the "description" column from the parsed DataFrame is selected.
"""

words_df = select_df.withColumn("words", split(select_df['description'], "[\\s,;.]+"))
"""
The words in the "description" column are split using a regular expression pattern for whitespaces, commas, semicolons, and periods.
"""

exploded_df = words_df.select(explode(words_df.words).alias("word"))
"""
The array of words is exploded into separate rows, with each row containing a single word.
"""

lower_df = exploded_df.selectExpr("lower(word) as word")
"""
All words are converted to lowercase.
"""

filtered_df = lower_df.filter(lower_df["word"] == "apple")
"""
Rows where the word is "apple" are filtered.
"""

counts = filtered_df.groupBy("word").count()
"""
The occurrences of each word are counted.
"""

final_df= counts.selectExpr("to_json(struct(*)) AS value")
logger.info(f"Spark data manupulation is finished ...")
"""
The result is converted to JSON format, and the column is renamed as "value."
"""

#query = counts.writeStream \
#    .outputMode("complete") \
#    .trigger(processingTime="2 seconds") \
#    .format("console") \
#    .start()

query = final_df.writeStream \
    .trigger(processingTime="2 seconds") \
    .outputMode("complete") \
    .format("kafka") \
    .option("topic", "resulttopic") \
    .option("kafka.bootstrap.servers", "192.168.67.7:9092,192.168.67.5:9092,192.168.67.6:9092") \
    .option("checkpointLocation", "file:///home/hadoop-user/spark-check-out") \
    .start()
logger.info(f"Streaming query object is beginning ... ")
"""
This block configures the output for the streaming query, specifying the trigger interval, output mode, 
output format (Kafka), target topic, Kafka bootstrap servers, and a checkpoint location.
"""

# Wait for the termination of the query
logger.info(f"Spark stream writing {final_df} to resulttopic ...")
query.awaitTermination()
logger.info(f"Spark stream writing {final_df} to resulttopic2 ...")
"""
The code waits for the termination of the streaming query to prevent the program from exiting immediately. 
The streaming query continues to run until manually stopped.
"""