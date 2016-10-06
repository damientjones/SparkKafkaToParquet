package app.metrics

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import app.schema.BatchMetrics
import app.util.{SparkContextUtil, YamlUtil}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.scheduler._

import scala.collection.concurrent.TrieMap

protected class StreamingMetrics(sc: SparkContext) extends StreamingListener {

  private val batchMetricsMetadata = YamlUtil.getConfigs.cassandraTables.get("batch_metrics")

  private object enum {
    sealed trait Status
    case object SUBMITTED extends Status
    case object STARTED extends Status
    case object COMPLETE extends Status
  }

  private val session = SparkContextUtil.getCassandraConnector.openSession()
  private val insert = YamlUtil.getConfigs.insertStatement
  private val stmt = session.prepare(insert.format(batchMetricsMetadata.keyspace,
    batchMetricsMetadata.table,
    batchMetricsMetadata.fields,
    batchMetricsMetadata.fields.split(",").map(x => "?").mkString(",")))

  private val sdf = new SimpleDateFormat("yyyyMMdd")
  private val appName: String = sc.appName
  private val applicationId: String = sc.applicationId
  private val master: String = sc.master
  private val totSchedDelayDly = sc.accumulator(0L)
  private val totPrcsgDelayDly = sc.accumulator(0L)
  private val totalDelayDly = sc.accumulator(0L)
  private val totRecsPrcsdDly = sc.accumulator(0L)
  private val subBatchMap = TrieMap.empty[Long, BatchMetrics]
  private val startedBatchMap = TrieMap.empty[Long, BatchMetrics]

  private def removeOldPrcsgBatches(time: Long) {
    startedBatchMap
      .keys
      .filter(x => x < time)
      .foreach(x => startedBatchMap.remove(x))
  }

  private def getBatchMetrics(status: enum.Status, batchInfo: BatchInfo) = {
    val time = batchInfo.batchTime.milliseconds
    val batchMetrics = status match {
      case enum.SUBMITTED =>
        val date = sdf.format(new Date(time))
        val batchMetrics = BatchMetrics(date,
          appName,
          new Timestamp(time),
          "SUBMITTED",
          applicationId,
          master)
        batchMetrics.totalDelayDly = totalDelayDly.value
        batchMetrics.totPrcsgDelayDly = totPrcsgDelayDly.value
        batchMetrics.totRecsPrcsdDly = totRecsPrcsdDly.value
        batchMetrics.totSchedDelayDly = totSchedDelayDly.value
        if (batchMetrics.totalDelayDly != 0) {
          batchMetrics.avgThroughputDly = batchMetrics.totRecsPrcsdDly / batchMetrics.totalDelayDly
        } else {
          batchMetrics.avgThroughputDly = batchMetrics.totRecsPrcsdDly / 1
        }
        batchMetrics.recordCount = batchInfo.numRecords
        subBatchMap.put(time, batchMetrics)
        batchMetrics
      case enum.STARTED =>
        val batchMetrics = subBatchMap.get(time).get
        subBatchMap.remove(time)
        batchMetrics.status = "STARTED"
        batchMetrics.schedDelay = batchInfo.schedulingDelay.get
        totSchedDelayDly += batchMetrics.schedDelay
        batchMetrics.totSchedDelayDly = totSchedDelayDly.value
        startedBatchMap.put(time, batchMetrics)
        removeOldPrcsgBatches(time)
        batchMetrics
      case enum.COMPLETE =>
        val batchMetrics = startedBatchMap.get(time).get
        startedBatchMap.remove(time)
        batchMetrics.status = "COMPLETED"
        batchMetrics.prcsgDelay = batchInfo.processingDelay.get
        batchMetrics.totalDelay = batchMetrics.schedDelay + batchMetrics.prcsgDelay
        if (batchMetrics.totalDelay != 0) {
          batchMetrics.throughput = batchMetrics.recordCount / batchMetrics.totalDelay
        } else {
          batchMetrics.throughput = batchMetrics.recordCount / 1
        }
        totPrcsgDelayDly += batchMetrics.prcsgDelay
        totalDelayDly += batchMetrics.totalDelayDly
        totRecsPrcsdDly += batchMetrics.recordCount
        batchMetrics.totRecsPrcsdDly = totRecsPrcsdDly.value
        batchMetrics.totPrcsgDelayDly = totPrcsgDelayDly.value
        batchMetrics.totalDelayDly = totalDelayDly.value
        if (batchMetrics.totalDelayDly != 0) {
          batchMetrics.avgThroughputDly = batchMetrics.totRecsPrcsdDly / batchMetrics.totalDelayDly
        } else {
          batchMetrics.avgThroughputDly = batchMetrics.totRecsPrcsdDly / 1
        }
        batchMetrics
    }
    batchMetrics.productIterator.map(x => x.asInstanceOf[Object]).toList
    val statement = stmt.bind(batchMetrics.productIterator.map(x => x.asInstanceOf[Object]).toSeq: _*)
    session.execute(statement)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    getBatchMetrics(enum.COMPLETE, batchCompleted.batchInfo)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    getBatchMetrics(enum.STARTED, batchStarted.batchInfo)
    SparkMetrics.setBatchTime(batchStarted.batchInfo.batchTime.milliseconds)
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    getBatchMetrics(enum.SUBMITTED, batchSubmitted.batchInfo)
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

