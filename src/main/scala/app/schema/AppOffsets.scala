package app.schema

import org.apache.spark.streaming.kafka.OffsetRange

case class AppOffsets(appName: String,
                      var offsets: Option[Array[OffsetRange]])
