package app.util

import scala.reflect.ClassTag

object FunctionUtil {
  def unboxMap[K, V](map: Map[K, Option[V]], key: K)(implicit tagKey: ClassTag[K], tagValue: ClassTag[V]): V = {
    map.get(key).get.get
  }
}
