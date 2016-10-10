package app.schema

import scala.reflect._
import scala.reflect.runtime.universe._

case class TableMetadata(table: String,
                         fields: Map[Int, FieldMetadata])

object TableMetadata {
  private def toLower(string: String) = string.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase().trim

  private def getClassName[T](cls: ClassTag[T]) = toLower(cls.runtimeClass.getSimpleName)

  private def getValue(ann: Annotation): String = {
    ann.tree.children.tail.collect({
      case Literal(Constant(id: String)) => id
    }).head
  }

  private def getAnnotation(ann: Annotation): String = {
    ann.tree.tpe.erasure.toString.split('.') match {
      case x if x.length > 0 => x.last
      case _ => ""
    }
  }

  private def getAnnotationInfo(ann: Annotation): (String, String) = {
    (getAnnotation(ann),
      getValue(ann))
  }

  private def getClassFields[T](typ: TypeTag[T]) = {
    typ.tpe.members.collect {
      case x: TermSymbol if x.isVal || x.isVar => x
    }.toSeq.reverse.zipWithIndex.map(x => {
      (x._2, FieldMetadata(toLower(x._1.name.toString),
        x._1.annotations.map(y => getAnnotationInfo(y)).toMap))
    }).toMap
  }

  def apply[T](implicit cls: ClassTag[T], typ: TypeTag[T]): TableMetadata = {
    TableMetadata(getClassName(cls),
      getClassFields(typ))
  }
}