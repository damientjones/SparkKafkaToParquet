package app.util

import java.sql.Timestamp
import java.text.SimpleDateFormat

import app.metrics.SparkMetrics
import app.schema.Obj1
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf

object CreateParquetUtil {
  val sql = SparkContextUtil.getSqlContext
  import sql.implicits._
  val dateFormat = new SimpleDateFormat("yyyyMMdd")
  def dateToString(time:Timestamp) : String = dateFormat.format(time)
  val dateUdf = udf(dateToString _)

  def writeObj1Parquet(rdd: RDD[String]) {
    val fileName = YamlUtil.getConfigs.directory + "\\" + "OBJ1"
    SparkMetrics.setExecutorInfo("Creating parquet file")
    rdd.flatMap(x => {
      Obj1.extract(x)
    })
      .toDF
      .withColumn("date",dateUdf($"time"))
      .write
      .mode(SaveMode.Append)
      .partitionBy("date")
      .parquet(fileName)
  }

  def writeFile(rdd: RDD[String])(implicit appName: String) {
    appName match {
      case "obj1App" => writeObj1Parquet(rdd)
    }
  }

}