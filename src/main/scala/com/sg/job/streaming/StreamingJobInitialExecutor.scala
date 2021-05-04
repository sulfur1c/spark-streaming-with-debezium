package com.sg.job.streaming

import com.sg.utils.DebeziumDeltaFormatter
import com.sg.wrapper.SparkSessionWrapper
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingJobInitialExecutor extends App with SparkSessionWrapper {

  val currentDirectory = new java.io.File(".").getCanonicalPath
  val kafkaReaderConfig = KafkaReaderConfig("localhost:29092", "dbserver1.inventory.customers")
  val jdbcConfig = JDBCConfig(url = "jdbc:postgresql://localhost:5432/test")
  new StreamingJobInitialExecutor(spark, kafkaReaderConfig, currentDirectory + "/checkpoint/job", jdbcConfig).execute()
}

class StreamingJobInitialExecutor(spark: SparkSession, kafkaReaderConfig: KafkaReaderConfig, checkpointLocation: String, jdbcConfig: JDBCConfig) {

  def execute(): Unit = {
    // read data from kafka and parse them
    val transformDF = read()
      .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "topic")

    // Write the output of a streaming aggregation query into Delta table
    transformDF.writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("append")
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
    if (!microBatchOutputDF.isEmpty) {
      microBatchOutputDF.write
        .format("delta")
        .save("hdfs://192.168.0.18:9000/mnt/delta/events")
    }
  }
}