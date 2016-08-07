package app.metrics

import org.apache.spark.streaming.scheduler._

class StreamingMetrics extends StreamingListener {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    println("on batch completed")
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    println("on batch started")
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    println("on batch submitted")
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    println("error")
  }

}
