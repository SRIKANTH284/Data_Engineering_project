from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Example').getOrCreate()
schema = spark.read.option('header', True).csv('batch_input/data.csv').schema
df_batch = spark.readStream.option('header', True).option('maxFilesPerTrigger',
    '1').schema(schema).csv('stream_input/')
df_filtered = df_batch.filter(df_batch['value'] > 50)
df_grouped = df_filtered.groupBy('category').count()
query = df_grouped.writeStream.outputMode('complete').format('console').start()
query.awaitTermination()
