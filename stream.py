from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('BatchProcessingExample').getOrCreate()
df_batch = spark.readStream.option('header', True).csv('data/full_data.csv')
df_filtered = df_batch.filter(df_batch['value'] > 50)
df_grouped = df_filtered.groupBy('category').count()
df_grouped.writeStream.csv('batch_output/')
