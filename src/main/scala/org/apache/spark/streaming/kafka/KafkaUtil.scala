package org.apache.spark.streaming.kafka

import app.enums.OffsetType
import app.enums.OffsetType._
import app.schema.{Checkpoint, KafkaMessage}
import app.util.{FunctionUtil, SparkContextUtil, YamlUtil}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream

object KafkaUtil {
  private var kafkaParams: Map[String, String] = _
  private var stream: InputDStream[KafkaMessage] = _
  private var varsSet: Boolean = _

  private def getEarliestOffsets(kafkaParams: Map[String, String], topics: String): Map[TopicAndPartition, Option[Long]] = {
    getOffsets(kafkaParams, topics, EARLIEST)
  }

  private def getLatestOffsets(kafkaParams: Map[String, String], topics: String): Map[TopicAndPartition, Option[Long]] = {
    getOffsets(kafkaParams, topics, LATEST)
  }

  private def determineOffset(checkpoint: (TopicAndPartition, Option[Long]), earliest: Long, latest: Long): (TopicAndPartition, Long) = {
    val offset = checkpoint._2 match {
      case x if x.isEmpty => earliest
      case x if x.get > latest => YamlUtil.getConfigs.getOffsetOverride match {
        case LATEST => latest
        case _ => earliest
      }
      case default => default.get
    }
    (checkpoint._1, offset)
  }

  private def getCheckpointOffsets(kafkaParams: Map[String, String], topics: String): Map[TopicAndPartition, Long] = {
    val checkpoint = getOffsets(kafkaParams, topics, CHECKPOINT)
    val latest = getLatestOffsets(kafkaParams: Map[String, String], topics: String)
    val earliest = getEarliestOffsets(kafkaParams: Map[String, String], topics: String)
    checkpoint.map(x => determineOffset(x,
      FunctionUtil.extractFromMap(earliest, x._1),
      FunctionUtil.extractFromMap(latest, x._1)))
  }

  private def getOffsets(kafkaParams: Map[String, String], topics: String, offsetType: OffsetType): Map[TopicAndPartition, Option[Long]] = {
    val kc = new KafkaCluster(kafkaParams)
    checkErrors(kc.getPartitions(topics.split(",").toSet).right.map(x => {
      offsetType match {
        case EARLIEST => checkErrors(kc.getEarliestLeaderOffsets(x)).map(x => x._1 -> Some(x._2.offset))
        case CHECKPOINT => Checkpoint.getOffsets(x)
        case LATEST => checkErrors(kc.getLatestLeaderOffsets(x)).map(x => x._1 -> Some(x._2.offset))
      }
    }))
  }

  private def checkErrors[T](result: Either[KafkaCluster.Err, T]): T = {
    result.fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok
    )
  }

  def getOffsets(rdd: RDD[KafkaMessage]): Array[OffsetRange] = {
    rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  }

  def createStream(brokerList: String, topicNames: String) = {
    if (!varsSet) {
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => KafkaMessage(mmd.topic, mmd.offset, mmd.key(), mmd.message())
      kafkaParams = Map[String,String]("metadata.broker.list"-> brokerList)
      val offsets = getCheckpointOffsets(kafkaParams, topicNames)
      stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, KafkaMessage](
      SparkContextUtil.getStreamingContext, kafkaParams, offsets, messageHandler)
      varsSet = true
    }
  }

  def getStream: InputDStream[KafkaMessage] = {
    stream
  }
}
