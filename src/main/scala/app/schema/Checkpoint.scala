package app.schema

import app.util.{CassandraUtil, SparkContextUtil, YamlUtil}
import kafka.common.TopicAndPartition
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka.OffsetRange

case class Checkpoint(app_name: String,
                      topic_name: String,
                      partition: Int,
                      checkpoint_time: Long,
                      start_offset: Long,
                      end_offset: Long,
                      completion_time: Option[Long] = None)

object Checkpoint {
  val csc = SparkContextUtil.getSqlContext

  import csc.implicits._

  val sc = SparkContextUtil.getStreamingContext.sparkContext

  def apply(appName: String, offset: OffsetRange): Checkpoint = {
    Checkpoint(appName,
      offset.topic,
      offset.partition,
      System.currentTimeMillis(),
      offset.fromOffset,
      offset.untilOffset)
  }

  def apply(appName: String, offset: OffsetRange, date: Long): Checkpoint = {
    Checkpoint(appName,
      offset.topic,
      offset.partition,
      System.currentTimeMillis(),
      offset.fromOffset,
      offset.untilOffset,
      Some(date))
  }

  def writeOffsets(appName: String, offsets: Array[OffsetRange]) {
    writeToCassandra(offsets.map(x => apply(appName, x)))
  }

  def completeOffsets(appName: String, offsets: Array[OffsetRange]) {
    writeToCassandra(offsets.map(x => apply(appName, x, System.currentTimeMillis())))
  }

  private def toSnakeCase(field: String) = field.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase().replace("$", "")

  private def writeToCassandra(checkpoints: Array[Checkpoint]) {
    CassandraUtil.saveDataframe("checkpoint", sc.parallelize(checkpoints).toDF)
  }

  private def map(row: Row) = {
    val checkPoint = Checkpoint(row.getAs[String]("app_name"),
      row.getAs[String]("topic_name"),
      row.getAs[Int]("partition"),
      row.getAs[Long]("checkpoint_time"),
      row.getAs[Long]("start_offset"),
      row.getAs[Long]("end_offset"))
    ((checkPoint.app_name, checkPoint.topic_name, checkPoint.partition), checkPoint)
  }

  private def getData = {
    CassandraUtil.getDataframe("checkpoint").map(x => map(x)).collectAsMap()
  }

  def getOffsets(topicPartitionSet: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    val checkpoints = getData
    val appName = YamlUtil.getConfigs.getAppName
    topicPartitionSet.map(x => {
      val offset = checkpoints.getOrElse((appName, x.topic, x.partition), null)
      (x, offset)
    })
    null
  }
}
