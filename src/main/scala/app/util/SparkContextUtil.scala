package app.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.kafka.KafkaUtil
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.JavaConverters._

object SparkContextUtil {
  private var sparkConf: SparkConf = _
  private var streamingContext: StreamingContext = _
  private var cassandraSqlContext: CassandraSQLContext = _
  private var varsSet: Boolean = false

  private def getBatchInterval(unit: String, interval: Int) = {
    unit match {
      case "Seconds" => Seconds(interval)
      case "Minutes" => Minutes(interval)
    }
  }

  private def getConfigs() = {
    val configs = YamlUtil.getConfigs
    val appName = configs.getAppName
    val master = configs.getMaster
    val sparkConfigs = configs.getSparkConfigs.asScala.toMap
    val streamConfigs = configs.getKafkaAppConfigs.get(configs.getAppName)
    val batchInterval = getBatchInterval(streamConfigs.get("batchIntervalUnit"),
      streamConfigs.get("batchInterval").toInt)
    val topicList = streamConfigs.get("topicList")
    val brokerList = streamConfigs.get("brokerList")
    (appName, master, sparkConfigs, batchInterval, topicList, brokerList)
  }

  def createContext {
    if (!varsSet) {
      val (appName, master, sparkConfigs, interval, topicList, brokerList) = getConfigs()
      sparkConf = new SparkConf().setAppName(appName).setMaster(master)
      sparkConfigs.foreach(x => sparkConf.set(x._1, x._2))
      streamingContext = new StreamingContext(sparkConf, interval)
      cassandraSqlContext = new CassandraSQLContext(streamingContext.sparkContext)
      KafkaUtil.createStream(brokerList, topicList)
      varsSet = true
    }
  }

  def getStreamingContext = {
    streamingContext
  }

  def getSqlContext = {
    cassandraSqlContext
  }
}
