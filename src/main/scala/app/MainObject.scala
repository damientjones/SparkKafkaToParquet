package app

import app.metrics.{SparkMetrics, StreamingMetrics}
import app.schema.{Checkpoint, KafkaMessage}
import app.util._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtil, OffsetRange}

object MainObject {

  def build(appName: String) = {
    YamlUtil.setYamlConfigs(appName)
    SparkContextUtil.createContext
    val ssc = SparkContextUtil.getStreamingContext
    ssc.addStreamingListener(StreamingMetrics.getMetrics(ssc.sparkContext)) //Streaming metrics class
    ssc.sparkContext.addSparkListener(SparkMetrics.getMetrics) //Spark metrics class
    ssc
  }

  def getJson(rec: KafkaMessage): Array[String] = {
    Array[String](rec.message)
  }

  def main (args:Array[String]): Unit = {
    val ssc = build(args(0))
    implicit val appName = YamlUtil.getConfigs.getAppName
    var offsets: Array[OffsetRange] = null
    val stream = KafkaUtil.getStream
    stream.foreachRDD { rdd =>
      offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      Checkpoint.writeOffsets(appName, offsets)
    }
    stream.flatMap(x => getJson(x))
      .foreachRDD(x => {
        CreateParquetUtil.writeFile(x)
        Checkpoint.completeOffsets(appName, offsets)
      })
    ssc.start()
    ssc.awaitTermination()
  }

}
