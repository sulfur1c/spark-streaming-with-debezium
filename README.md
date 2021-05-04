# spark-streaming-with-debezium

## Prerequisites
- Docker
- IDE (with SBT and JDK)

## Setup:
Run ```docker-compose up```

### Setting-up HDFS
Install hdfs using your local ip as host to be visible from Dremio (docker) https://www.datasciencecentral.com/profiles/blogs/how-to-install-and-run-hadoop-on-windows-for-beginners

If the installation goes well you can check HDFS here: http://"yourLocalIP":9870/

### Setting-up Dremio
Delta Lake is currently offered in Preview and can be enabled by setting the support key “dremio.deltalake.enabled” to true. https://www.dremio.com/announcing-dremio-february-2021

If the installation goes well you can check Dremio here: http://localhost:9047//

### Setting-up Superset with Dremio libraries
I have used this docker image https://hub.docker.com/r/wtkwsk/superset-dremio-drivers and this instructions to set up and initialize Superset https://hub.docker.com/r/apache/superset and https://www.dremio.com/tutorials/dremio-apache-superset/ for Dremio's connector https://www.dremio.com/tutorials/dremio-apache-superset/

## Running the app
At this point you have all necessary tools up and running. Here we are going to describe step by step how to do things running

- run ```StreamingJobInitialExecutor```

- Check if there some connector, result should be empty array: ```curl -H "Accept:application/json" localhost:8083/connectors/```
  
- Create connector: ```curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "inventory-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "root", "database.password": "debezium", "database.server.id": "184054", "database.server.name": "dbserver1", "database.include.list": "inventory", "database.history.kafka.bootstrap.servers": "kafka:9092", "database.history.kafka.topic": "dbhistory.inventory" } }'```

After some time processing you should see initial MYSQL data in HDFS, check it with spark-shell or Dremio HDFS connector

- Spark Shell: ```spark-shell --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"```

- Dremio: Remember to setup properly refresh metadata interval to 1 min once you are creating HDFS Dremio's connector

We are ready to streaming, just leave running StreamingJobExecutor and do changes in MYSQL customers table, you should see changes in Dremio and HDFS

- Run ```StreamingJobExecutor```

### More info:
https://medium.com/@suchit.g/spark-streaming-with-kafka-connect-debezium-connector-ab9163808667
