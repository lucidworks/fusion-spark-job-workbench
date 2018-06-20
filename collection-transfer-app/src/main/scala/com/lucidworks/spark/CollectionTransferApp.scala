package com.lucidworks.spark

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.UUID.randomUUID

import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.cli.{CommandLine, GnuParser, HelpFormatter, Options, ParseException, Option => CliOpt}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.util.RTimer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

object CollectionTransferApp extends LazyLogging {
  val appName = "fusion-collection-xfer"
  val xferIdField = "_lw_xfer_id_s"

  def main(args: Array[String]) {
    val cli = processCommandLineArgs(getOptions, args).get
    val verbose = cli.hasOption("verbose")

    val spark = initSparkSession(cli)
    if (verbose) {
      logger.info(s"Initialized SparkSession using: ${spark.sparkContext.getConf.toDebugString}")
    }

    val destinationSolrClusterZk = cli.getOptionValue("destinationSolrClusterZk")
    val sourceSolrClusterZk = cli.getOptionValue("sourceSolrClusterZk")
    val sourceCollection = cli.getOptionValue("sourceCollection")
    val destinationCollection = cli.getOptionValue("destinationCollection", sourceCollection)
    val sourceQuery = cli.getOptionValue("sourceQuery", "*:*")
    val timestampFieldName = cli.getOptionValue("timestampField", "timestamp_tdt")
    val batchSize = cli.getOptionValue("batchSize", "10000")
    val findNewOnly = ("true" == cli.getOptionValue("findNewOnly", "true"))
    val optimizeOutput = cli.getOptionValue("optimizeOutput", "0").toInt
    val useNaturalID = cli.getOptionValue("useNaturalID", "").trim
    val naturalIdFields = if (!useNaturalID.isEmpty) useNaturalID.split(",").toSeq else Seq.empty

    logger.info(s"Fusion collection transfer running with config: sourceSolrClusterZk=${sourceSolrClusterZk}, sourceCollection=${sourceCollection}, destinationSolrClusterZk=${destinationSolrClusterZk}, destinationCollection=${destinationCollection}, sourceQuery=${sourceQuery}, findNewOnly=${findNewOnly}")

    val sourceFilter = if (findNewOnly) {
      val timestampFilter = getTimestampFilterFromDestination(destinationSolrClusterZk, destinationCollection, timestampFieldName)
      logger.info(s"Read timestamp filter ${timestampFilter} from destination collection ${destinationCollection} on ${destinationSolrClusterZk}")
      val hasNewDocsCheck = new SolrQuery(sourceQuery)
      hasNewDocsCheck.setRows(0)
      hasNewDocsCheck.addFilterQuery(timestampFilter)
      val newDocsInSource = SolrQuerySupport.getNumDocsFromSolr(sourceCollection, sourceSolrClusterZk, Some(hasNewDocsCheck))
      logger.info(s"Found ${newDocsInSource} new docs in source collection ${sourceCollection} to transfer to ${destinationCollection} in cluster ${destinationSolrClusterZk}")
      (Some(timestampFilter), newDocsInSource)
    } else {
      (None, 0L)
    }

    if (!findNewOnly || sourceFilter._2 > 0L) {
      var readFromSourceClusterOpts = Map("zkhost" -> sourceSolrClusterZk, "collection" -> sourceCollection, "query" -> sourceQuery, "flatten_multivalued" -> "false")
      if (sourceFilter._1.isDefined) {
        readFromSourceClusterOpts = readFromSourceClusterOpts ++ Map("solr.params" -> s"fq=${sourceFilter._1.get}")
      }
      logger.info(s"Reading from source collection using options: ${readFromSourceClusterOpts.mkString(", ")}")
      val sourceCollectionDF = spark.read.format("solr").options(readFromSourceClusterOpts).load
      if (verbose) {
        logger.info(s"Source collection has schema: ${sourceCollectionDF.schema.mkString("; ")}")
      }

      val xferUUID = randomUUID.toString
      logger.info(s"Using ${xferIdField}=${xferUUID} for tracking writes from this transfer process.")

      val rtimer = new RTimer
      val writeToDestinationClusterOpts =
        Map("zkhost" -> destinationSolrClusterZk, "collection" -> destinationCollection, "batch_size" -> batchSize, "commit_within" -> "30000")

      val outputDF = if (!naturalIdFields.isEmpty) {
        sourceCollectionDF.withColumn("id", concat_ws("|", naturalIdFields.map(f => coalesce(col(f),lit(""))):_*))
      } else {
        sourceCollectionDF
      }
      outputDF.withColumn(xferIdField, lit(xferUUID)).write.format("solr").options(writeToDestinationClusterOpts).save
      val solrClient = SolrSupport.getCachedCloudClient(destinationSolrClusterZk)
      solrClient.commit(destinationCollection)

      if (optimizeOutput > 0) {
        logger.info(s"Optimizing destination collection ${destinationCollection} to '${optimizeOutput}' segments")
        solrClient.optimize(destinationCollection, false, false, optimizeOutput)
      }
      // get a count of docs written to the dest; may be more than newDocsInSource if some docs were committed after we checked for new docs in source
      val docsWritten = checkDestinationDocCount(solrClient, destinationCollection, s"${xferIdField}:${xferUUID}")
      logger.info(s"Took ${rtimer.getTime} ms to write ${docsWritten} to ${destinationCollection} with tracking ${xferIdField}=${xferUUID}")
    } // else no docs in the source cluster to sync ... fail fast ;-)
    spark.stop()
    System.exit(0)
  }

  def checkDestinationDocCount(solrClient: CloudSolrClient, destinationCollection: String, query: String) : Long = {
    val solrQuery = new SolrQuery(query)
    solrQuery.setRows(0)
    val qr: QueryResponse = solrClient.query(destinationCollection, solrQuery)
    qr.getResults.getNumFound
  }

  def initSparkSession(cli: CommandLine) : SparkSession = {
    val sparkConf = new SparkConf()
    if (cli.getOptionValue("sparkConf") != null) {
      val sparkConfPropsFile = new File(cli.getOptionValue("sparkConf"))
      if (!sparkConfPropsFile.isFile) {
        throw new IllegalArgumentException(s"Additional Spark config file ${sparkConfPropsFile.getAbsolutePath} not found!")
      }
      val props = new Properties()
      props.load(new BufferedReader(new InputStreamReader(new FileInputStream(sparkConfPropsFile), "UTF-8")))
      sparkConf.setAll(props.asInstanceOf[java.util.Map[String, String]].toMap)
      logger.info(s"Added custom configuration properties to SparkConf: ${sparkConf.toDebugString}")
    }
    SparkSession.builder().config(sparkConf).appName(appName).getOrCreate()
  }

  def getTimestampFilterFromDestination(zkhost:String, collection:String, timestampField: String = "timestamp_tdt") : String = {
    // do a top query for the latest doc in the destination collection
    val solrQuery = new SolrQuery("*:*")
    solrQuery.setRows(1)
    solrQuery.setFields(timestampField)
    solrQuery.setSort(timestampField, SolrQuery.ORDER.desc)
    logger.info(s"Sending query to find latest doc on destination collection ${collection} ...")
    val qr: QueryResponse = SolrSupport.getCachedCloudClient(zkhost).query(collection, solrQuery)
    val nowTs = DateTimeFormatter.ISO_INSTANT.format(Instant.now)
    if (qr.getResults.getNumFound != 0) {
      val maxDate = qr.getResults.get(0).getFirstValue(timestampField).asInstanceOf[java.util.Date]
      if (maxDate != null) {
        val maxTs = DateTimeFormatter.ISO_INSTANT.format(maxDate.toInstant)
        return s"$timestampField:{$maxTs TO $nowTs]"
      }
    } else {
      logger.info(s"No docs found in destination collection ${collection}")
    }
    return s"$timestampField:[* TO $nowTs]" // all rows
  }

  def getOptions: Array[CliOpt] = {
    Array(
      CliOpt.builder()
          .hasArg().required(true)
          .desc("ZooKeeper connection string for the Solr cluster this app transfers data to")
          .longOpt("destinationSolrClusterZk").build,
      CliOpt.builder()
          .hasArg().required(true)
          .desc("ZooKeeper connection string for the Solr cluster this app transfers data from")
          .longOpt("sourceSolrClusterZk").build,
      CliOpt.builder()
          .hasArg()
          .desc("Name of the Solr collection on the destination cluster to write data to; uses source name if not provided")
          .longOpt("destinationCollection").build,
      CliOpt.builder()
          .hasArg().required(true)
          .desc("Name of the Solr collection on the source cluster to read data from")
          .longOpt("sourceCollection").build,
      CliOpt.builder()
          .hasArg()
          .desc("Query to source collection for docs to transfer; uses *:* if not provided")
          .longOpt("sourceQuery").build,
      CliOpt.builder()
          .hasArg()
          .desc("Flag to indicate if this app should look for new docs in the source using the latest timestamp in the destination; defaults to true, set to false to skip this check and pull all docs that match the source query")
          .longOpt("findNewOnly").build,
      CliOpt.builder()
          .hasArg()
          .desc("Timestamp field name on docs; defaults to 'timestamp_tdt'")
          .longOpt("timestampField").build,
      CliOpt.builder()
          .hasArg()
          .desc("Batch size for writing docs to the destination cluster; defaults to 10000")
          .longOpt("batchSize").build,
      CliOpt.builder()
          .hasArg()
          .desc("Additional Spark configuration properties file")
          .longOpt("sparkConf").build,
      CliOpt.builder()
          .hasArg()
          .desc("Compute Natural Doc ID by concatenating fields")
          .longOpt("useNaturalID").build,
      CliOpt.builder()
          .hasArg
          .desc("Optimize the destination collection to a configured number of segments. Skips optimization if value is zero")
          .longOpt("optimizeOutput").build
    )
  }

  def processCommandLineArgs(appOptions: Array[CliOpt], args: Array[String]): Option[CommandLine] = {
    val options = new Options()
    options.addOption("h", "help", false, "Print this message")
    options.addOption("v", "verbose", false, "Generate verbose log messages")
    appOptions.foreach(options.addOption(_))
    try {
      val cli = (new GnuParser()).parse(options, args);
      if (cli.hasOption("help")) {
        (new HelpFormatter()).printHelp(appName, options)
        System.exit(0)
      }
      return Some(cli)
    } catch {
      case pe: ParseException => {
        if (args.filter(p => p == "-h" || p == "-help").isEmpty) {
          // help not requested ... report the error
          System.err.println("Failed to parse command-line arguments due to: " + pe.getMessage())
        }
        (new HelpFormatter()).printHelp(appName, options)
        System.exit(1)
      }
      case e: Exception => throw e
    }
    None
  }
}

