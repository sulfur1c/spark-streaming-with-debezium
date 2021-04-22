package com.sg.job.streaming

import com.sg.wrapper.SparkSessionWrapper
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType}
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
  val deltaTable = DeltaTable.forPath(spark, "/mnt/delta/events")

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
        "s.key = t.key")
      .whenMatched("s.deleted = true")
      .delete()
      .whenMatched()
      .updateExpr(Map("key" -> "s.key", "time" -> "s.time", "deleted" -> "s.deleted", "value" -> "s.value"))
      .whenNotMatched("s.deleted = false")
      .insertExpr(Map("key" -> "s.key", "time" -> "s.time", "deleted" -> "s.deleted", "value" -> "s.value"))
      .execute()
  }

  // DataFrame with changes having following columns
  // - key: key of the change
  // - time: time of change for ordering between changes (can replaced by other ordering id)
  // - value: updated or inserted value if key was not deleted
  // - deleted: true if the key was deleted, false if the key was inserted or updated
  def debeziumTodeltaFormat(microBatchInputDF: DataFrame, batchId: Long): DataFrame = {
    var microBatchFinalOutputDF: DataFrame = spark.emptyDataFrame
    if (!microBatchInputDF.isEmpty) {
      val rowIterator = microBatchInputDF.toLocalIterator()
      while (rowIterator.hasNext) {
        val row = rowIterator.next()
        // Filter delete double result
        if (row.getString(1) != null) {
          val dataFrame: DataFrame = extractRow(row)
          if (microBatchFinalOutputDF.columns.size > 0) {
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

  def extractRow(row: Row): DataFrame = {
    var microBatchOutputDF: DataFrame = spark.emptyDataFrame

    val jsonKey = JSON.parseFull(row.getString(0))
    val jsonValue = JSON.parseFull(row.getString(1))

    val key = jsonKey.get.asInstanceOf[Map[String, String]].get("payload")
                     .get.asInstanceOf[Map[String, Double]].get("id").get

    val valuePayload = jsonValue.get.asInstanceOf[Map[String, String]].get("payload")
                                .get.asInstanceOf[HashMap.HashTrieMap[String, Any]]
    val time = valuePayload("ts_ms")
    val value = valuePayload("after").asInstanceOf[Map[String, String]]
    val deleted = valuePayload("op").toString.equalsIgnoreCase("d")

    val someData = Seq(
      Row(key, time, deleted)
    )

    val someSchema = List(
      StructField("key", DoubleType, true),
      StructField("time", DoubleType, true),
      StructField("deleted", BooleanType, true)
    )

    microBatchOutputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(someSchema)
    )

    if (value != null) {
      microBatchOutputDF = microBatchOutputDF.withColumn(
      "value",
        struct(
          lit(value.get("id").get).as("id"),
          lit(value.get("first_name").get).as("first_name"),
          lit(value.get("last_name").get).as("last_name"),
          lit(value.get("email").get).as("email")
        )
      )
    } else {
      microBatchOutputDF = microBatchOutputDF.withColumn("value", lit(null: String))
    }
    microBatchOutputDF
  }
}