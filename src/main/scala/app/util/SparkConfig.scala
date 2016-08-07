package app.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkConfig {
  private var sparkConf: SparkConf = null
  private var streamingContext : StreamingContext = null
  private var sqlContext : SQLContext = null

  def createContext {
    if (sparkConf == null) {
      sparkConf = new SparkConf().setAppName(ConfigUtil("appName")).setMaster(ConfigUtil("master"))
      streamingContext = new StreamingContext(sparkConf,Seconds(ConfigUtil("batchSize").toInt))
      sqlContext = new SQLContext(streamingContext.sparkContext)
    }
  }

  def getStreamingContext = {
    streamingContext
  }
  def getSqlContext = {
    sqlContext
  }
}
