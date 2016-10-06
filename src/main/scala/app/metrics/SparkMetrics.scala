package app.metrics

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import app.schema.JobMetrics
import app.util.{SparkContextUtil, YamlUtil}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

import scala.collection.concurrent.TrieMap

protected class SparkMetrics(sc: SparkContext) extends SparkListener {

  private val insert = YamlUtil.getConfigs.insertStatement
  private val jobMetricsMetadata = YamlUtil.getConfigs.cassandraTables.get("job_metrics")
  private val jobStartInsertFields = jobMetricsMetadata.fields.split(",").filter(x => x != "job_end_time" && x != "prcsg_time").mkString(",")
  private val sdf = new SimpleDateFormat("yyyyMMdd")
  private val appName: String = sc.appName
  private val applicationId: String = sc.applicationId
  private val master: String = sc.master
  private var batchTime: Long = _
  private var jobInfo: String = _

  private val startedBatchMap = TrieMap.empty[Int, JobMetrics]
  private val session = SparkContextUtil.getCassandraConnector.openSession()

  private val jobStartStmt = session.prepare(insert.format(jobMetricsMetadata.keyspace,
    jobMetricsMetadata.table,
    jobStartInsertFields,
    jobStartInsertFields.split(",").map(x => "?").mkString(",")))
  private val jobEndStmt = session.prepare(insert.format(jobMetricsMetadata.keyspace,
    jobMetricsMetadata.table,
    jobMetricsMetadata.fields,
    jobMetricsMetadata.fields.split(",").map(x => "?").mkString(",")))

  override def onJobStart(jobStart: SparkListenerJobStart) {
    if (jobStart.stageInfos.nonEmpty) {
      jobInfo = jobStart.stageInfos.head.name
    }
    val date = sdf.format(new Date(batchTime))
    val jobMetrics = JobMetrics(date,
      appName,
      jobStart.jobId,
      new Timestamp(batchTime),
      applicationId,
      master,
      jobInfo,
      new Timestamp(jobStart.time))
    startedBatchMap.put(jobStart.jobId, jobMetrics)
    val statement = jobStartStmt.bind(jobMetrics.productIterator.take(8).map(x => x.asInstanceOf[Object]).toSeq: _*)
    session.execute(statement)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val jobMetrics = startedBatchMap.get(jobEnd.jobId).get
    startedBatchMap.remove(jobEnd.jobId)
    jobMetrics.jobEndTime = new Timestamp(jobEnd.time)
    jobMetrics.prcsgTime = jobEnd.time - jobMetrics.jobStartTime.getTime
    val statement = jobEndStmt.bind(jobMetrics.productIterator.map(x => x.asInstanceOf[Object]).toSeq: _*)
    session.execute(statement)
  }

}

object SparkMetrics {
  lazy val cc = SparkContextUtil.getCassandraConnector
  var metrics: SparkMetrics = null

  def getMetrics(sc: SparkContext): SparkMetrics = {
    if (metrics == null) {
      metrics = new SparkMetrics(sc)
    }
    metrics
  }

  def setBatchTime(batchTime: Long) {
    metrics.batchTime = batchTime
  }

  def setJobInfo(jobInfo: String) {
    metrics.jobInfo = jobInfo
  }
}