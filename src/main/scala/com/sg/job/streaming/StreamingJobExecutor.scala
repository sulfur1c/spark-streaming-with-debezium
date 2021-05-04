package com.sg.job.streaming

import com.sg.utils.DebeziumDeltaFormatter
import com.sg.wrapper.SparkSessionWrapper
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingJobExecutor extends App with SparkSessionWrapper {

  val currentDirectory = new java.io.File(".").getCanonicalPath
  val kafkaReaderConfig = KafkaReaderConfig("localhost:29092", "dbserver1.inventory.customers")
  new StreamingJobExecutor(spark, kafkaReaderConfig).execute()
}

class StreamingJobExecutor(spark: SparkSession, kafkaReaderConfig: KafkaReaderConfig) {

  val deltaTable: DeltaTable = DeltaTable.forPath(spark, "hdfs://192.168.0.18:9000/mnt/delta/events")

  def execute(): Unit = {
    // read data from kafka and parse them
    val transformDF = read()
      .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "topic")

    // Write the output of a streaming aggregation query into Delta table
    transformDF.writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
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
      .option("failOnDataLoss", false)
      .load()
  }

  // Function to upsert microBatchOutputDF into Delta table using merge
  def upsertToDelta(microBatchInputDF: DataFrame, batchId: Long) {
    val microBatchOutputDF = DebeziumDeltaFormatter(spark).debeziumTodeltaFormat(microBatchInputDF, batchId)
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
}