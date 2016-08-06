package app

import app.util.{CreateParquet, KafkaStream, SparkConfig, ConfigUtil}
import app.util.WriteMetrics
object MainObject {

  def build (inputTopic:String,propsFile:String) = {
    ConfigUtil.setAppVars(inputTopic,propsFile)
    SparkConfig.createContext
    KafkaStream.createStream
    val ssc = SparkConfig.getStreamingContext
    ssc.addStreamingListener(new WriteMetrics)
    ssc
  }

  def getFileName (implicit appName : String): String = {
    val appName = ConfigUtil("appName")
    ConfigUtil("directory") + "\\" + appName
  }

  def getJson (rec:(String,String)) : Array[String] = {
    Array[String](rec._2)
  }

  def main (args:Array[String]): Unit = {
    val ssc = build(args(0),args(1))
    implicit val appName = ConfigUtil("appName")
    KafkaStream.getStream
      .flatMap(x => getJson(x))
      .foreachRDD(x => CreateParquet.writeFile(x, getFileName))
    ssc.start()
    ssc.awaitTermination()
  }

}
