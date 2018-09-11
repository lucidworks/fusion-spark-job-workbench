package com.lucidworks.spark.fusion

import java.io.InputStream
import java.net.URLEncoder

import org.apache.commons.cli
import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory



class IndexDirectoryToFusion {}

object IndexDirectoryToFusion {

  val logger = LoggerFactory.getLogger(classOf[IndexDirectoryToFusion])

  def indexToFusion(
       path: String,
       fusionHostAndPortList: String,
       sparkContext: SparkContext,
       pipelineId: String,
       parserId: String,
       collectionId: String,
       partitions: Int,
       username: String,
       password: String,
       defaultFS: Option[String]): Unit = {
    val pipelinePath : String = s"api/apollo/index-pipelines/${pipelineId}/collections/${collectionId}/index?parserId=${parserId}"
    val conf = new Configuration()
    if (defaultFS.isDefined) {
      conf.set("fs.defaultFS", defaultFS.get)
    }
    val fs = FileSystem.get(conf)
    val basePath = new Path(path)
    val ls = fs.listStatus(basePath)
    val paths = ls.map(_.getPath).toSeq
      .map(_.toString)
    logger.info(s"Found ${paths.size} files at path ${path}")
    val rddPaths : RDD[String] = sparkContext.parallelize(paths, partitions)
    rddPaths.foreachPartition(it => {
      val fusionPipelineClientV2 = new FusionPipelineClientV2(fusionHostAndPortList, username, password, "native")
      it.foreach(p => {
        val nconf = new Configuration()
        if (defaultFS.isDefined) {
          nconf.set("fs.defaultFS", defaultFS.get)
        }
        val nfs = FileSystem.get(nconf)
        val encodedPath = URLEncoder.encode(p, "UTF-8")
        val pis = new PortableInputStream(nfs, new Path(p))
        val pipelinePathWithId = s"${pipelinePath}&contentSourceName=${encodedPath}"
        logger.info(s"Streaming file ${p} to index pipeline path ${pipelinePathWithId}")
        try {
          fusionPipelineClientV2.postInputStreamToPipeline(pipelinePathWithId, pis)
        } catch {
          case e: Exception =>
            logger.error(s"Exception indexing file ${p}. Exception: ${e.getMessage}")
        }
      })
    })
    logger.info(s"Finished indexing files at directory ${path}")
  }

  class PortableInputStream(fileSystem: FileSystem, path: Path) {

    def open(): InputStream =  {
      fileSystem.open(path).getWrappedStream
    }
  }

  def main(args: Array[String]): Unit = {
    val commandLine: CommandLine = getCommandLine(args)
    val collection = commandLine.getOptionValue("collection")
    val directory = commandLine.getOptionValue("directory")
    val pipelineId = commandLine.getOptionValue("pipeline", collection)
    val parserId = commandLine.getOptionValue("parser", collection)
    val hostPortList = commandLine.getOptionValue("host", "http://localhost:8764")
    val defaultFS = Some(commandLine.getOptionValue("defaultfs", null))
    val partitions = commandLine.getOptionValue("partitions", "10")
    val username = commandLine.getOptionValue("username", "admin")
    val password = commandLine.getOptionValue("password", "password123")

    val sparkSession = SparkSession.builder().appName("Azure Indexing").getOrCreate()
    val sparkContext = sparkSession.sparkContext
    indexToFusion(directory, hostPortList, sparkContext, pipelineId, parserId, collection, partitions.toInt, username, password, defaultFS)
  }

  def getCommandLine(strings: Array[String]): CommandLine = {
    val options = new Options
    val collection = new cli.Option("c", "collection", true, "Collection to index to")
    collection.setRequired(true)

    val directory = new cli.Option("d", "directory", true, "Directory to crawl")
    directory.setRequired(true)

    val pipeline = new cli.Option("p", "pipeline", true, "Pipeline to use")
    val parserId = new cli.Option("ps", "parser", true, "Parser to use")
    val fusionHostPortList = new cli.Option("fh", "host", true, "Fusion Host and Port to use")
    val defaultFS = new cli.Option("fs", "defaultfs", true, "Default file system to set (E.g., s3 or azure)")
    val partitions = new cli.Option("pts", "partitions", true, "Number of partitions")
    val username = new cli.Option("u", "username", true, "Fusion proxy username")
    val password = new cli.Option("pwd", "password", true, "Fusion proxy password")

    options.addOption(collection)
    options.addOption(directory)
    options.addOption(pipeline)
    options.addOption(parserId)
    options.addOption(fusionHostPortList)
    options.addOption(defaultFS)
    options.addOption(partitions)
    options.addOption(username)
    options.addOption(password)

    val commandLineParser = new DefaultParser
    val helpFormatter = new HelpFormatter
    try {
      commandLineParser.parse(options, strings)
    } catch {
      case e: Exception =>
        logger.error(s"Error parsing command line args. Exception: ${e}")
        helpFormatter.printHelp("Crawl and index directory", options)
        throw e
    }
  }
}
