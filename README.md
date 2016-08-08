# SparkKafkaToParquet

## Sample JSON messages:

OBJ1:
{"string":"99224","time":1572006802000,"int":3969,"bool":false}

OBJ2:
{"time":1572006802000,"string":"99224","int":3969,"bool":false}

## Inputs to main method:
Topic name, properties file:
ex: OBJ1 c:\app.properties

## The project includes a StreamingListener and a SparkListener
These will print out messages. This can also be used to log metrics to the log file or to a database table.

## A few notes

If using the default app.properties file the parquet files will be written to C:\data\OBJ1

The parquet files will also be partitioned by extracting YYYYMMDD from the time field

Project uses SBT Assembly and will create an UBER Jar, label dependencies as provided as needed

##Sample Kafka Commands
#Windows (Unix/Linux are similar)
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

.\bin\windows\kafka-server-start.bat .\config\server.properties

.\bin\windows\kafka-topics.bat --create --topic OBJ1 --zookeeper localhost:2181 --replication-factor 1 --partitions 1

.\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181

.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic OBJ1

.\bin\windows\kafka-console-consumer.bat --zookeeper localhost:2181 --topic OBJ1
