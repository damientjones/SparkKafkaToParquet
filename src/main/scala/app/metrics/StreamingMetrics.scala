package app.metrics

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import app.schema.JobMetrics
import app.util.{SparkContextUtil, YamlUtil}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.scheduler._

import scala.collection.concurrent.TrieMap

protected class StreamingMetrics(sc: SparkContext) extends StreamingListener {

  val jobMetricsMetadata = YamlUtil.getConfigs.cassandraTables.get("jobMetrics")
  val jobMetricsTableName = jobMetricsMetadata.get("table")
  val jobMetricsKeyspace = jobMetricsMetadata.get("keyspace")
  val jobMetricsFields = jobMetricsMetadata.get("fields")

  private object enum {

    sealed trait Status

    case object SUBMITTED extends Status

    case object STARTED extends Status

    case object COMPLETE extends Status

  }

  private val session = SparkContextUtil.getCassandraConnector.openSession()
  private val insert = YamlUtil.getConfigs.insertStatement
  private val stmt = session.prepare(insert.format(jobMetricsKeyspace,
    jobMetricsTableName,
    jobMetricsFields,
    jobMetricsFields.split(",").map(x => "?").mkString(",")))

  private val sdf = new SimpleDateFormat("yyyyMMdd")
  private val appName: String = sc.appName
  private val applicationId: String = sc.applicationId
  private val master: String = sc.master
  private val totSchedDelayDly = sc.accumulator(0L)
  private val totPrcsgDelayDly = sc.accumulator(0L)
  private val totalDelayDly = sc.accumulator(0L)
  private val totRecsPrcsdDly = sc.accumulator(0L)
  private val subBatchMap = TrieMap.empty[Long, JobMetrics]
  private val startedBatchMap = TrieMap.empty[Long, JobMetrics]

  private def removeOldPrcsgBatches(time: Long) {
    startedBatchMap
      .keys
      .filter(x => x != time)
      .foreach(x => startedBatchMap.remove(x))
  }

  private def getJobMetrics(status: enum.Status, batchInfo: BatchInfo) = {
    val time = batchInfo.batchTime.milliseconds
    val jobMetrics = status match {
      case enum.SUBMITTED => {
        val date = sdf.format(new Date(time))
        val jobMetrics = JobMetrics(date,
          appName,
          new Timestamp(time),
          "SUBMITTED",
          applicationId,
          master)
        jobMetrics.totalDelayDly = totalDelayDly.value
        jobMetrics.totPrcsgDelayDly = totPrcsgDelayDly.value
        jobMetrics.totRecsPrcsdDly = totRecsPrcsdDly.value
        jobMetrics.totSchedDelayDly = totSchedDelayDly.value
        if (jobMetrics.totalDelayDly != 0) {
          jobMetrics.avgThroughputDly = jobMetrics.totRecsPrcsdDly / jobMetrics.totalDelayDly
        } else {
          jobMetrics.avgThroughputDly = jobMetrics.totRecsPrcsdDly / 1
        }
        jobMetrics.recordCount = batchInfo.numRecords
        subBatchMap.put(time, jobMetrics)
        jobMetrics
      }
      case enum.STARTED => {
        val jobMetrics = subBatchMap.get(time).get
        subBatchMap.remove(time)
        jobMetrics.status = "STARTED"
        jobMetrics.schedDelay = batchInfo.schedulingDelay.get
        totSchedDelayDly += jobMetrics.schedDelay
        jobMetrics.totSchedDelayDly = totSchedDelayDly.value
        startedBatchMap.put(time, jobMetrics)
        removeOldPrcsgBatches(time)
        jobMetrics
      }
      case enum.COMPLETE => {
        val jobMetrics = startedBatchMap.get(time).get
        startedBatchMap.remove(time)
        jobMetrics.status = "COMPLETED"
        jobMetrics.prcsgDelay = batchInfo.processingDelay.get
        jobMetrics.totalDelay = jobMetrics.schedDelay + jobMetrics.prcsgDelay
        if (jobMetrics.totalDelay != 0) {
          jobMetrics.throughput = jobMetrics.recordCount / jobMetrics.totalDelay
        } else {
          jobMetrics.throughput = jobMetrics.recordCount / 1
        }
        totPrcsgDelayDly += jobMetrics.prcsgDelay
        totalDelayDly += jobMetrics.totalDelayDly
        totRecsPrcsdDly += jobMetrics.recordCount
        jobMetrics.totRecsPrcsdDly = totRecsPrcsdDly.value
        jobMetrics.totPrcsgDelayDly = totPrcsgDelayDly.value
        jobMetrics.totalDelayDly = totalDelayDly.value
        if (jobMetrics.totalDelayDly != 0) {
          jobMetrics.avgThroughputDly = jobMetrics.totRecsPrcsdDly / jobMetrics.totalDelayDly
        } else {
          jobMetrics.avgThroughputDly = jobMetrics.totRecsPrcsdDly / 1
        }
        jobMetrics
      }
    }
    jobMetrics.productIterator.map(x => x.asInstanceOf[Object]).toList
    val statement = stmt.bind(jobMetrics.productIterator.map(x => x.asInstanceOf[Object]).toSeq: _*)
    println(statement)
    session.execute(statement)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    getJobMetrics(enum.COMPLETE, batchCompleted.batchInfo)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    getJobMetrics(enum.STARTED, batchStarted.batchInfo)
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    getJobMetrics(enum.SUBMITTED, batchSubmitted.batchInfo)
  }

}

object StreamingMetrics {
  var metrics: Option[StreamingMetrics] = None

  def getMetrics(sc: SparkContext): StreamingMetrics = {
    if (metrics.isEmpty) {
      metrics = Some(new StreamingMetrics(sc))
    }
    metrics.get
  }
}

