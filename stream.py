
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("StreamingProcessingExample").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("value", StringType(), True)
])

# Read streaming data
df_stream_raw = spark.readStream.option("header", True).option("maxFilesPerTrigger", 1).schema(schema).csv("stream_input/")

# Select and cast
df_stream = df_stream_raw.select(
    col("category"),
    col("value").cast("double")
)

# Apply transformations
df_filtered = df_stream.filter(col('value') > 50)
df_grouped = df_filtered.groupBy("category").count()

# Write output to console
query = df_grouped.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
