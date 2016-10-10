package app.metrics

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import app.schema.JobMetrics
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

import scala.collection.concurrent.TrieMap

protected class SparkMetrics(sc: SparkContext) extends ProcessCaseClass[JobMetrics] with SparkListener {

  private val sdf = new SimpleDateFormat("yyyyMMdd")
  private val appName: String = sc.appName
  private val applicationId: String = sc.applicationId
  private val master: String = sc.master
  private var batchTime: Long = _
  private var jobInfo: String = _

  private val startedBatchMap = TrieMap.empty[Int, JobMetrics]

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
    insert(jobMetrics)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val jobMetrics = startedBatchMap.get(jobEnd.jobId).get
    startedBatchMap.remove(jobEnd.jobId)
    jobMetrics.jobEndTime = new Timestamp(jobEnd.time)
    jobMetrics.prcsgTime = jobEnd.time - jobMetrics.jobStartTime.getTime
    insert(jobMetrics)
  }

}

object SparkMetrics {
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
}