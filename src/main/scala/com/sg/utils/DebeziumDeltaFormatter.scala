package com.sg.utils

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

case class DebeziumDeltaFormatter(spark: SparkSession) {

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
