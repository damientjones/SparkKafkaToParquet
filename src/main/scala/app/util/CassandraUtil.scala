package app.util

import org.apache.spark.sql.{DataFrame, SaveMode}

object CassandraUtil {
  def getDataframe(table: String) = {
    val tableMetadata = YamlUtil.getConfigs.cassandraTables.get(table)
    SparkContextUtil.getSqlContext.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableMetadata.get("table"),
        "keyspace" -> tableMetadata.get("keyspace"),
        "pushdown" -> "false"))
      .load()
      .selectExpr(tableMetadata.get("fields").split(",").toSeq: _*)
  }

  def saveDataframe(table: String, df: DataFrame) {
    val tableMetadata = YamlUtil.getConfigs.cassandraTables.get(table)
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableMetadata.get("table"),
        "keyspace" -> tableMetadata.get("keyspace")))
      .mode(SaveMode.Overwrite)
      .save()
  }
}
