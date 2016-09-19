package app.metrics

import org.apache.spark.scheduler._

protected class SparkMetrics extends SparkListener {

  private var executorInfo: String = null

  override def onJobStart(jobStart: SparkListenerJobStart) {
    println("job started: " + jobStart.jobId)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    println("job ended: " + jobEnd.jobId)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    println("stage completed on attempt: " + stageCompleted.stageInfo.attemptId)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    //Output line where stage was submitted from (helps identify what operation is being done)
    println("stage submitted details: " + stageSubmitted.stageInfo.details.split("\n").filter(x => x.contains("app"))(0) + " number of tasks: " + stageSubmitted.stageInfo.numTasks)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    println("task started, task id: " + taskStart.taskInfo.taskId)
  }

}

object SparkMetrics {
  var metrics: SparkMetrics = null

  def getMetrics: SparkMetrics = {
    if (metrics == null) {
      metrics = new SparkMetrics
    }
    metrics
  }

  def setExecutorInfo(info: String) {
    metrics.executorInfo = info
  }
}