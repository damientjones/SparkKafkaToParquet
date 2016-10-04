package app

import app.metrics.{SparkMetrics, StreamingMetrics}
import app.schema.{AppOffsets, Checkpoint, KafkaMessage}
import app.util._
import org.apache.spark.streaming.kafka.KafkaUtil

object MainObject {

  def build(appName: String, fileName: String) = {
    YamlUtil.parseYaml(appName, fileName)
    SparkContextUtil.createStreamingContext
    SparkContextUtil.createStream
    val ssc = SparkContextUtil.getStreamingContext
    ssc.addStreamingListener(StreamingMetrics.getMetrics(ssc.sparkContext))
    ssc.sparkContext.addSparkListener(SparkMetrics.getMetrics)
    ssc
  }

  def getJson(rec: KafkaMessage): Array[String] = {
    Array[String](rec.message)
  }

  def main (args:Array[String]): Unit = {
    val ssc = build(args(0), args(1))
    implicit val appName = YamlUtil.getConfigs.appName
    val offsets = AppOffsets(appName, None)
    val stream = KafkaUtil.getStream
    stream.foreachRDD { rdd =>
      offsets.offsets = Some(KafkaUtil.getOffsets(rdd))
      Checkpoint.writeOffsets(offsets)
    }
    stream.flatMap(x => getJson(x))
      .foreachRDD(x => {
        CreateParquetUtil.writeFile(x)
        Checkpoint.completeOffsets(offsets)
      })
    ssc.start()
    ssc.awaitTermination()
  }

}
