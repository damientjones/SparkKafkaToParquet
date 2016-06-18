package app.util

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaStream {
  private var kafkaParams : Map[String,String] = null
  private var topicsSet : Set[String] = null
  private var stream : InputDStream[(String,String)] = null

  def createStream {
    kafkaParams = Map[String,String]("metadata.broker.list"->ConfigUtil("kafkaBrokers"))
    topicsSet = Set[String](ConfigUtil("topicName"))
    stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      SparkConfig.getStreamingContext, kafkaParams, topicsSet)
  }

  def getStream = {
    stream
  }
}
