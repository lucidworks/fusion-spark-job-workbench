package com.lucidworks.spark.examples

import com.lucidworks.apollo.client.resources.{BlobClientResource, CollectionClientResource}
import com.lucidworks.spark.FusionClient
import com.lucidworks.spark.util.FusionUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class CountDocsInCollection

object CountDocsInCollection {

  val logger = LoggerFactory.getLogger(classOf[CountDocsInCollection])
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
        .builder
        .appName("CountDocsJavaExample")
        .getOrCreate()

//    logger.info(s"Spark conf dump: ${spark.sparkContext.getConf.toDebugString}")
//    logger.info(s"System properties: ${System.getProperties.keySet()}")
//    logger.info(s"Env properties: ${System.getenv().keySet()}")
    val df = spark.read.format("solr").option("collection", "system_logs").load()
//    df.count()
    logger.info("Document count is  " + df.count())
    spark.stop()
  }

}
