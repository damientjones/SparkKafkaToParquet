package app.util

import org.apache.spark.streaming.scheduler.{StreamingListenerBatchSubmitted, StreamingListenerBatchStarted, StreamingListenerBatchCompleted, StreamingListener}

class WriteMetrics extends StreamingListener {

  override def onBatchCompleted(batchCompleted:StreamingListenerBatchCompleted) {
    println("on batch completed")
  }

  override def onBatchStarted(batchStarted : StreamingListenerBatchStarted) {
    println("on batch started")
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    println("on batch submitted")
  }

}
