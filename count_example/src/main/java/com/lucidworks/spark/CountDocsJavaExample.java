package com.lucidworks.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class CountDocsJavaExample {

  private static Logger logger = LoggerFactory.getLogger(CountDocsJavaExample.class);

  public static void main(String[] args) {
    SparkSession sparkSession = SparkSession
        .builder()
        .appName("CountDocsJavaExample")
        .getOrCreate();

    Dataset dataset = sparkSession.read().format("solr").option("collection", "system_logs").load();
    logger.info("No. of docs in collection logs are " + dataset.count());
    sparkSession.stop();
  }
}
