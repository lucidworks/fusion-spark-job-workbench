from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python basic example") \
    .getOrCreate()

df = spark.read.format("solr").option("collection", "system_history").load()
print("No. of docs in logs collection {}".format(df.count()))
spark.stop()

