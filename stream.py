# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Step 1: Initialize Spark session
spark = SparkSession.builder \
    .appName("StreamingProcessingExample") \
    .getOrCreate()

# Step 2: Define paths
input_stream_path = "stream_input/"
output_stream_path = "stream_output/"
checkpoint_path = "stream_checkpoint/"

# Step 3: Define a loose schema (read all columns as string initially)
schema = StructType([
    StructField("id", StringType(), True),         # we don't care about this
    StructField("category", StringType(), True),
    StructField("value", StringType(), True)        # read value as string first to avoid errors
])

# Step 4: Read streaming data
df_stream_raw = (
    spark.readStream
    .option("header", True)
    .option("maxFilesPerTrigger", 1)
    .schema(schema)
    .csv(input_stream_path)
)

# Step 5: Select only needed columns and cast value properly
df_stream = df_stream_raw.select(
    col("category"),
    col("value").cast("double")
)

# Step 6: Apply transformations
df_filtered = df_stream.filter(col('value') > 50)
df_grouped = df_filtered.groupBy("category").count()

# Step 7: Write streaming output to console
query = df_grouped.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Step 8: Keep the stream alive
query.awaitTermination()
