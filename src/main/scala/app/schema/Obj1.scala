package app.schema

import java.sql.Timestamp

import app.jso4s.format.JsonFormats
import com.fasterxml.jackson.databind.JsonMappingException
import org.json4s.jackson.JsonMethods._

case class Obj1(string: Option[String],
                time: Option[Timestamp],
                int: Option[Int],
                bool: Option[Boolean])

object Obj1 extends JsonFormats{

  def extract(json: String): Option[Obj1] = {
    try {
      val parsedJson = parse(json)
      Some(parsedJson.extract[Obj1])
    } catch {
      case x: JsonMappingException => None
    }
  }
}