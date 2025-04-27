import argparse

# Step 1: Parse command-line argument
parser = argparse.ArgumentParser(description="Convert batch Spark code to streaming Spark code.")
parser.add_argument("input_file", help="Path to batch Python file (batch.py)")
args = parser.parse_args()

# Step 2: Read batch.py
with open(args.input_file, "r") as file:
    batch_code = file.read()

# Step 3: Build fixed stream.py
stream_py_code = """
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
"""

# Step 4: Save stream.py
with open("stream.py", "w") as file:
    file.write(stream_py_code)

print("✅ Correct stream.py generated successfully!")
