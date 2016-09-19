package app.schema.format

import java.sql.{Date, Timestamp}

import app.enums.ObjType
import org.json4s.JsonAST.{JInt, JString}
import org.json4s.{CustomSerializer, DefaultFormats}

trait JsonFormats {

  def timeStampFromLong(int : BigInt) = {
    new Timestamp(int.toLong)
  }

  case object DateSerializer extends CustomSerializer[Timestamp](format => (
    {
      case JInt(i) => timeStampFromLong(i)
    },
    {
      case d: Date => JString(d.toString)
    }
    )
  )

  implicit val formats = DefaultFormats + DateSerializer + new EnumSerializer[ObjType]
}