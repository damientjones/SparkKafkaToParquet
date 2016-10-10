package app.annotation

sealed trait AppAnnotations extends scala.annotation.StaticAnnotation

case class DefaultValue(value: String) extends AppAnnotations
