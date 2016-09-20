# SparkKafkaToParquet

## Sample JSON messages:

OBJ1: <br />
{"string":"99224","time":1572006802000,"int":3969,"bool":false}

## Inputs to main method:
App name: <br />
ex: obj1App

## The project includes a StreamingListener and a SparkListener
These will print out messages. This can also be used to log metrics to the log file or to a database table.

## A few notes

If using the default config.yaml file the parquet files will be written to C:\data\OBJ1

The parquet files will also be partitioned by extracting YYYYMMDD from the time field

Project uses SBT Assembly and will create an UBER Jar, label dependencies as provided as needed

This project depends on a Cassandra instance as well as the tables in the tables.sql file existing

## General KafkaDirectStream information:
The application manages offsets by syncing them with the Cassandra table test_keyspace.checkpoint. <br />
On Start up the app will read the offsets from this table and do one of three things for each partition in Kafka: <br />
1) Get earliest offset for a partition with no offsets in the checkpoint table <br />
    a) This may happen if a topic was dropped and recreated or a new partition was added <br />
2) Get earliest or latest offsets for a partition if the current offsets in the checkpoint table are past the latest offsets <br />
    a) Uses offsetOverride parameter in config.yaml to determine earliest or latest <br />
3) Use existing offsets from the checkpoint table for a partition <br />

##Sample Kafka Commands
###Windows (Unix/Linux are similar)
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties <br />
.\bin\windows\kafka-server-start.bat .\config\server.properties <br />
.\bin\windows\kafka-topics.bat --create --topic OBJ1 --zookeeper localhost:2181 --replication-factor 1 --partitions 2 <br />
.\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181 <br />
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic OBJ1 <br />
.\bin\windows\kafka-console-consumer.bat --zookeeper localhost:2181 --topic OBJ1
