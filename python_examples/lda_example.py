from pyspark.ml.clustering import LDA
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("LDA example") \
    .getOrCreate()

df = spark.read.format("solr").option("collection", "logs").load()

lda = LDA(k=10, maxIter=10)