package app.metrics

import org.apache.spark.scheduler._

class SparkMetrics extends SparkListener {

  //batch -> job -> stage -> executor

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

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    println("task ended duration: " + taskEnd.taskInfo.duration + "ms")
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
    println("executor metrics update for executor: " + executorMetricsUpdate.execId)
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) {
    println("executor added on host: " + executorAdded.executorInfo.executorHost)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) {
    println("executor removed")
  }
}
