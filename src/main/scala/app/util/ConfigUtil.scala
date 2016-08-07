package app.util

import scala.collection.mutable.HashMap
import scala.io.Source

object ConfigUtil {

  private var configMap : Map[String,String] = null

  private def splitString(line:String) = {
    val array = line.split("=")
    (array(0),array(1))
  }

  def apply (key:String): String = {
    configMap.get(key).get
  }

  def setAppVars(inputTopic:String, propsFile:String) {
    if (configMap == null) {
      val map = HashMap[String,String]("topicName"->inputTopic)
      val appName = inputTopic
      map.put("appName",appName)
      Source.fromFile(propsFile)
        .getLines()
        .map(splitString)
        .foreach(x => map.put(x._1,x._2))
      val batchSize = map.get(s"${appName}.batchSize").get
      map.put("batchSize",batchSize)
      configMap = map.toMap[String,String]
    }
  }
}