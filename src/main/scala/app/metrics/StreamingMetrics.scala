package app.metrics

import org.apache.spark.streaming.scheduler._

class StreamingMetrics extends StreamingListener {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    println("on batch completed, processed " + batchCompleted.batchInfo.numRecords + " records in " + batchCompleted.batchInfo.processingDelay.get + "ms")
    println("scheduling delay: " + batchCompleted.batchInfo.schedulingDelay.get + "ms")
    println("total time to process batch: " + batchCompleted.batchInfo.totalDelay.get + "ms")
    batchCompleted.batchInfo.streamIdToInputInfo.map(x => x._2.metadata.foreach(println))
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    println("on batch started")
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    println("on batch submitted")
    batchSubmitted.batchInfo
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    println("error")
  }

}
