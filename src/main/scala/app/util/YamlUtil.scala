package app.util

import java.io.FileReader

import app.Config
import com.esotericsoftware.yamlbeans.YamlReader

import scala.collection.JavaConverters._

object YamlUtil {
  private var configInd: Boolean = false
  private var config: Config = _

  def parseYaml(appName: String, fileName: String) {
    if (!configInd) {
      val reader = new YamlReader(new FileReader(fileName))
      config = reader.read(classOf[Config])
      config.appName = appName
      configInd = true
    }
  }

  def getConfigs = {
    config
  }

  def getSparkConfigs = {
    config.sparkConfigs.asScala.toMap
  }
}