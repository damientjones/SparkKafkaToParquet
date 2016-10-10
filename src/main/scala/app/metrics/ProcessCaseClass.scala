package app.metrics

import app.TableInfo
import app.schema.TableMetadata
import app.util.{SparkContextUtil, YamlUtil}

import scala.collection.mutable.Queue
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class ProcessCaseClass[T <: Product](implicit val cls: ClassTag[T], implicit val typ: TypeTag[T]) {

  private def toLower(string: String) = string.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase().trim

  private val tableName = toLower(cls.runtimeClass.getSimpleName)
  private val insert = YamlUtil.getConfigs.insertStatement
  private val session = SparkContextUtil.getCassandraConnector.openSession()
  private val tableMetadata = TableMetadata[T]
  private val tableInfo: TableInfo = YamlUtil.getConfigs.cassandraTables.get(tableName)

  private def getFieldsAndValues(metrics: T) = {
    metrics.productIterator.zipWithIndex.flatMap(x => {
      val fieldMetadata = tableMetadata.fields.get(x._2)
      if (fieldMetadata.get.annotations.getOrElse("DefaultValue", "") != x._1.toString) {
        Some(fieldMetadata.get.field, x._1)
      } else {
        None
      }
    }).foldLeft(Queue.empty[String], Queue.empty[Object]) {
      case (queues, x) => {
        queues._1 += x._1
        queues._2 += x._2.asInstanceOf[Object]
        queues
      }
    }
  }

  protected def insert(metrics: T) {
    val (fields, values) = getFieldsAndValues(metrics)
    session.execute(session.prepare(insert.format(tableInfo.keyspace,
      tableInfo.table,
      fields.mkString(","),
      fields.map(x => "?").mkString(",")))
      .bind(values.toSeq: _*))
  }

}
