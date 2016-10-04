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
<UL>
<LI>If using the default config.yaml file the parquet files will be written to C:\data\OBJ1
<LI>The parquet files will also be partitioned by extracting YYYYMMDD from the time field
<LI>Project uses SBT Assembly and will create an UBER Jar, label dependencies as provided as needed
<LI>This project depends on a Cassandra instance as well as the tables in the tables.sql file existing
<LI>Direct stream uses a custom message handler that includes the topic name and offsets along with the key and message
<UL>
<LI>Please see createStream in KafkaUtil for usage and custom object
</UL>
</UL>

## General KafkaDirectStream app information:
The application manages offsets by syncing them with the Cassandra table test_keyspace.checkpoint. <br />
On Start up the app will read the offsets from this table and do one of three things for each partition in Kafka:
<OL>
<LI>Get earliest offset for a partition with no offsets in the checkpoint table
<UL>
<LI>This may happen if a topic was dropped and recreated or a new partition was added
</UL>
<LI>Get earliest or latest offsets for a partition if the current offsets in the checkpoint table are past the latest offsets
<UL>
<LI>Uses offsetOverride parameter in config.yaml to determine earliest or latest
</UL>
<LI>Use existing offsets from the checkpoint table for a partition
</OL>

The app uses a custom KafkaMessage object which contains the topic name, offset, key, and message for each message read from Kafka. This is through using a custom message handler and the MessageAndMetadata case class.

##Sample Kafka Commands
###Windows (Unix/Linux are similar)
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties <br />
.\bin\windows\kafka-server-start.bat .\config\server.properties <br />
.\bin\windows\kafka-topics.bat --create --topic OBJ1 --zookeeper localhost:2181 --replication-factor 1 --partitions 2 <br />
.\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181 <br />
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic OBJ1 <br />
.\bin\windows\kafka-console-consumer.bat --zookeeper localhost:2181 --topic OBJ1
