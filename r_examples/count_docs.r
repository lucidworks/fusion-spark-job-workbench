library(SparkR)

sparkR.session(appName = "R Spark SQL basic example")

df1 <- read.df("solr", collection = "logs")

print("Number of documents in collection logs is " + count(df1))
