from pyspark.sql import SparkSession

# Initialize Spark session and context
spark = SparkSession.builder.appName("ExampleRDD").getOrCreate()
sc = spark.sparkContext

# Read CSV as RDD
rdd = sc.textFile("batch_input/data.csv")

# Split by comma, filter and map
rdd_filtered = rdd.map(lambda line: line.split(",")) \
                            .filter(lambda fields: int(fields[2]) > 50) \
                            .map(lambda fields: (fields[1], 1)) .reduceByKey(lambda x, y: x + y)

# Save output
rdd_filtered.coalesce(1).saveAsTextFile("batch_output/output.csv")

