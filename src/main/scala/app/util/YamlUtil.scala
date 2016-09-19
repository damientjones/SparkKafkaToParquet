package app.util

import java.io.InputStream
import java.nio.file.{Files, Paths}

import app.Config
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.util.Try

object YamlUtil {
  private var config: Config = null

  def setYamlConfigs(appName: String) {
    if (config == null) {
      val in: InputStream = Try(Files.newInputStream(Paths.get("config.yaml"))).get
      config = new Yaml(new Constructor(classOf[Config])).load(in).asInstanceOf[Config]
      config.setAppName(appName)
    }
  }

  def getConfigs = {
    config
  }
}