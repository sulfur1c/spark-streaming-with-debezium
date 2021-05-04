package com.sg.job.streaming

case class JDBCConfig(url: String, user: String = "test", password: String = "Test123", tableName: String = "orders_it")

case class KafkaReaderConfig(kafkaBootstrapServers: String, topics: String, startingOffsets: String = "latest")

case class StreamingJobConfig(checkpointLocation: String, kafkaReaderConfig: KafkaReaderConfig)