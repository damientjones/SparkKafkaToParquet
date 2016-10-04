package app.util

import org.apache.spark.sql.{DataFrame, SaveMode}

object CassandraUtil {
  def getDataframe(table: String) = {
    val tableMetadata = YamlUtil.getConfigs.cassandraTables.get(table)
    SparkContextUtil.getSqlContext.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableMetadata.table,
        "keyspace" -> tableMetadata.keyspace,
        "pushdown" -> "false"))
      .load()
      .selectExpr(tableMetadata.fields.split(",").toSeq: _*)
  }

  def saveDataframe(table: String, df: DataFrame) {
    val tableMetadata = YamlUtil.getConfigs.cassandraTables.get(table)
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableMetadata.table,
        "keyspace" -> tableMetadata.keyspace))
      .mode(SaveMode.Overwrite)
      .save()
  }
}
