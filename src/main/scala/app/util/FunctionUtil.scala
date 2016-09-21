package app.util

object FunctionUtil {
  def extractFromMap[K, V] = (map: Map[K, Option[V]], key: K) => {
    map.get(key).get.get
  }
}
