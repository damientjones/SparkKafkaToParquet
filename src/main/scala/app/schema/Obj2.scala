package app.schema

import java.sql.Timestamp

import app.jso4s.format.JsonFormats
import com.fasterxml.jackson.databind.JsonMappingException
import org.json4s.jackson.JsonMethods._

case class Obj2(time: Option[Timestamp],
                string: Option[String],
                int: Option[Int],
                bool: Option[Boolean])

object Obj2 extends JsonFormats {

  def extract(json: String): Option[Obj2] = {
    try {
      val parsedJson = parse(json)
      Some(parsedJson.extract[Obj2])
    } catch {
      case x: JsonMappingException => None
    }
  }
}
