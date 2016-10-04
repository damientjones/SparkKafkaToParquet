package app.util

import java.io.FileReader

import app.Config
import com.esotericsoftware.yamlbeans.YamlReader

import scala.collection.JavaConverters._

object YamlUtil {
  private var configInd: Boolean = false
  private var config: Config = _

  def parseYaml(fileName: String) {
    if (!configInd) {
      val reader = new YamlReader(new FileReader("config.yaml"))
      config = reader.read(classOf[Config])
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