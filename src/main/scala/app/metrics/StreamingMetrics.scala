package app.metrics

import org.apache.spark.SparkContext
import org.apache.spark.streaming.scheduler._

protected class StreamingMetrics(sc: SparkContext) extends StreamingListener {

  println("Application Name: " + sc.appName)
  println("Application id: " + sc.applicationId)
  println("Master URL: " + sc.master)
  println("Spark Version: " + sc.version)
  println("Spark user: " + sc.sparkUser)

  var batchTime: Long = 0L

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    println("on batch completed, processed " + batchCompleted.batchInfo.numRecords + " records in " + batchCompleted.batchInfo.processingDelay.get + "ms")
    println("scheduling delay: " + batchCompleted.batchInfo.schedulingDelay.get + "ms")
    println("total time to process batch: " + batchCompleted.batchInfo.totalDelay.get + "ms")
    batchCompleted.batchInfo.streamIdToInputInfo.foreach(x => x._2.metadata.foreach(println))
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    println("on batch started")
    batchTime = batchStarted.batchInfo.batchTime.milliseconds
    batchStarted.batchInfo.streamIdToInputInfo.foreach(x => x._2.metadata.foreach(println))
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    println("on batch submitted")
    //batchSubmitted.batchInfo
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    println("error")
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

  def getBatchTime: Long = {
    metrics.get.batchTime
  }
}

