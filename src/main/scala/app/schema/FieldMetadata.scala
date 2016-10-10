package app.schema

case class FieldMetadata(field: String,
                         annotations: Map[String, String] = Map.empty)
