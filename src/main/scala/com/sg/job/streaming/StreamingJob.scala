package com.sg.job.streaming

import com.sg.wrapper.SparkSessionWrapper
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable.HashMap
import scala.util.parsing.json._

object StreamingJob extends App with SparkSessionWrapper {

  val currentDirectory = new java.io.File(".").getCanonicalPath
  val kafkaReaderConfig = KafkaReaderConfig("localhost:29092", "dbserver1.inventory.customers")
  val jdbcConfig = JDBCConfig(url = "jdbc:postgresql://localhost:5432/test")
  new StreamingJobExecutor(spark, kafkaReaderConfig, currentDirectory + "/checkpoint/job", jdbcConfig).execute()
}

case class JDBCConfig(url: String, user: String = "test", password: String = "Test123", tableName: String = "orders_it")

case class KafkaReaderConfig(kafkaBootstrapServers: String, topics: String, startingOffsets: String = "latest")

case class StreamingJobConfig(checkpointLocation: String, kafkaReaderConfig: KafkaReaderConfig)

class StreamingJobExecutor(spark: SparkSession, kafkaReaderConfig: KafkaReaderConfig, checkpointLocation: String, jdbcConfig: JDBCConfig) {
  val deltaTable: DeltaTable = DeltaTable.forPath(spark, "/mnt/delta/events")

  def execute(): Unit = {
    // read data from kafka and parse them
    val transformDF = read()
      .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "topic")

    // Write the output of a streaming aggregation query into Delta table
    transformDF.writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
      //.outputMode("append")
      .option("checkpointLocation", "/mnt/delta/events/_checkpoints/etl-from-json")
      .start()
      .awaitTermination()
  }

  def read(): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaReaderConfig.kafkaBootstrapServers)
      .option("subscribe", kafkaReaderConfig.topics)
      .option("startingOffsets", kafkaReaderConfig.startingOffsets)
      .load()
  }

  // Function to upsert microBatchOutputDF into Delta table using merge
  def upsertToDelta(microBatchInputDF: DataFrame, batchId: Long) {
    val microBatchOutputDF = debeziumTodeltaFormat(microBatchInputDF, batchId)
    /*if (!microBatchOutputDF.isEmpty) {
      microBatchOutputDF.write
        .format("delta")
        .save("/mnt/delta/events")
    }*/
    deltaTable.as("t")
      .merge(
        microBatchOutputDF.as("s"),
        "s.id = t.id")
      .whenMatched("s.deleted = true")
      .delete()
      .whenMatched()
      // TODO dynamic for every table
      .updateExpr(Map("id" -> "s.id", "first_name" -> "s.first_name", "last_name" -> "s.last_name", "email" -> "s.email"))
      .whenNotMatched()
      .insertExpr(Map("id" -> "s.id", "first_name" -> "s.first_name", "last_name" -> "s.last_name", "email" -> "s.email"))
      .execute()
  }

  def debeziumTodeltaFormat(microBatchInputDF: DataFrame, batchId: Long): DataFrame = {
    var microBatchFinalOutputDF: DataFrame = spark.emptyDataFrame
    if (!microBatchInputDF.isEmpty) {
      val rowIterator = microBatchInputDF.toLocalIterator()
      while (rowIterator.hasNext) {
        val row = rowIterator.next()
        // TODO Filter delete double result in connect ??
        if (row.getString(1) != null) {
          val dataFrame: DataFrame = extractRow(row)
          if (microBatchFinalOutputDF.columns.length > 0) {
            microBatchFinalOutputDF = microBatchFinalOutputDF.union(dataFrame)
          } else {
            microBatchFinalOutputDF = dataFrame
          }
        }
      }
    }
    microBatchFinalOutputDF.show()
    microBatchFinalOutputDF
  }

  // TODO extract to another class
  def extractRow(row: Row): DataFrame = {

    val jsonKey = JSON.parseFull(row.getString(0))
    val jsonValue = JSON.parseFull(row.getString(1))

    val id = jsonKey.get.asInstanceOf[Map[String, String]].get("payload")
                     .get.asInstanceOf[Map[String, Double]].get("id").get
    val valuePayload = jsonValue.get.asInstanceOf[Map[String, String]].get("payload")
                                .get.asInstanceOf[HashMap.HashTrieMap[String, Any]]

    val deleted = valuePayload("op").toString.equalsIgnoreCase("d")
    val value = valuePayload("after").asInstanceOf[Map[String, String]]

    // TODO dynamic for every table
    var firstName = ""
    var lastName = ""
    var email = ""
    if (value!=null) {
      firstName = value("first_name")
      lastName = value("last_name")
      email = value("email")
    }

    val someData = Seq(
      Row(id, firstName, lastName, email, deleted)
    )

    val someSchema = List(
      StructField("id", DoubleType, true),
      StructField("first_name", StringType, true),
      StructField("last_name", StringType, true),
      StructField("email", StringType, true),
      StructField("deleted", BooleanType, true)
    )

    var microBatchOutputDF: DataFrame = spark.emptyDataFrame

    microBatchOutputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(someSchema)
    )
    microBatchOutputDF
  }
}