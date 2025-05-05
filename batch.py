# Import SparkSession from PySpark
from pyspark.sql import SparkSession

# Initialize Spark session and context
spark = SparkSession.builder.appName("ExampleRDD").getOrCreate()
#work with RDDs
sc = spark.sparkContext

# Read input CSV as raw text RDD(each line is string)
rdd = sc.textFile("batch_input/data.csv")

#apply series of transformation
# Split by comma
#filter rows where 3rd column index2 >50
#create (k,1) pairs using 2nd column index 1 - counting
# reduce by key to count total occurrences per key
rdd_filtered = rdd.map(lambda line: line.split(",")) \
                            .filter(lambda fields: int(fields[2]) > 50) \
                            .map(lambda fields: (fields[1], 1)) .reduceByKey(lambda x, y: x + y)

# Save output
rdd_filtered.coalesce(1).saveAsTextFile("batch_output/output.csv")

