# SparkKafkaToParquet

## Sample JSON messages:

OBJ1:
{"string":"99224","time":1572006802000,"int":3969,"bool":false}

OBJ2:
{"time":1572006802000,"string":"99224","int":3969,"bool":false}

## Inputs to main method:
Topic name, properties file:
ex: OBJ1 c:\app.properties

## A few notes

If using the default app.properties file the parquet files will be written to C:\data\OBJ1

The parquet files will also be partitioned by extracting YYYYMMDD from the time field

Project uses SBT Assembly and will create an UBER Jar, label dependencies as provided as needed
