from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()

# Batch processing: Read CSV, filter, and group by category
schema=spark.read.option("header", True).csv("batch_input/data.csv").schema
df_batch = spark.read.option("header", True).csv("batch_input/data.csv")
df_filtered = df_batch.filter(df_batch['value'] > 50)
df_grouped = df_filtered.groupBy("category").count()


# Write the output to CSV
df_grouped.write.csv("batch_output/")