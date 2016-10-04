package app.util

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.kafka.KafkaUtil
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkContextUtil {
  lazy private val sparkConf: SparkConf = {
    val conf = new SparkConf().setAppName(YamlUtil.getConfigs.appName).setMaster(YamlUtil.getConfigs.master)
    YamlUtil.getSparkConfigs.foreach(x => conf.set(x._1, x._2))
    conf
  }
  lazy private val sc: SparkContext = new SparkContext(sparkConf)
  lazy private val csc: CassandraSQLContext = new CassandraSQLContext(sc)
  lazy private val cc: CassandraConnector = new CassandraConnector(CassandraConnectorConf(sparkConf))
  private var ssc: StreamingContext = _

  private def getBatchInterval(unit: String, interval: Int) = {
    unit match {
      case "Seconds" => Seconds(interval)
      case "Minutes" => Minutes(interval)
    }
  }

  private def setStreamingContext() = {

  }

  def createStreamingContext {
    val configs = YamlUtil.getConfigs
    val streamConfigs = configs.kafkaAppConfigs.get(configs.appName)
    val batchInterval = getBatchInterval(streamConfigs.batchIntervalUnit,
      streamConfigs.batchInterval.toInt)
    ssc = new StreamingContext(sc, batchInterval)
  }

  def createStream {
    val configs = YamlUtil.getConfigs
    val streamConfigs = configs.kafkaAppConfigs.get(configs.appName)
    KafkaUtil.createStream(streamConfigs.brokerList, streamConfigs.topicList)
  }

  def getStreamingContext = {
    ssc
  }

  def getSqlContext = {
    csc.setKeyspace(YamlUtil.getConfigs.defaultKeyspace)
    csc
  }

  def getCassandraConnector = {
    cc
  }
}
