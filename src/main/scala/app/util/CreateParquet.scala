package app.util

import java.sql.Timestamp
import java.text.SimpleDateFormat

import app.schema.{Obj1, Obj2}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf

object CreateParquet {
  val sql = SparkConfig.getSqlContext
  import sql.implicits._
  val dateFormat = new SimpleDateFormat("yyyyMMdd")
  def dateToString(time:Timestamp) : String = dateFormat.format(time)
  val dateUdf = udf(dateToString _)

  def writeObj1Parquet (rdd : RDD[String], fileName: String) {
    rdd.flatMap(x => Obj1.extract(x))
      .toDF
      .withColumn("date",dateUdf($"time"))
      .write
      .mode(SaveMode.Append)
      .partitionBy("date")
      .parquet(fileName)
  }

  def writeObj2Parquet (rdd : RDD[String], fileName: String) {
    rdd.flatMap(x => Obj2.extract(x))
      .toDF
      .withColumn("date",dateUdf($"time"))
      .write
      .mode(SaveMode.Append)
      .partitionBy("date")
      .parquet(fileName)
  }

  def writeFile (rdd : RDD[String], fileName: String)(implicit appName : String) {

    appName match {
      case "OBJ1" => writeObj1Parquet(rdd, fileName)
      case "OBJ2" => writeObj2Parquet(rdd, fileName)
    }
  }

}