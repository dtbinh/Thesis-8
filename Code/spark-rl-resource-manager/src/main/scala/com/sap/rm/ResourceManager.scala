package com.sap.rm

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.apache.spark.streaming.scheduler._

trait ResourceManager extends Spark with StreamingListener with SparkListenerTrait with ExecutorAllocator with ResourceManagerLogger {

  protected val config: ResourceManagerConfig
  import config._

  private lazy val statBuilder: StatBuilder = StatBuilder(ReportDuration)

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = logExecutorAdded(executorAdded)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = logExecutorRemoved(executorRemoved)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = logApplicationEnd(applicationEnd)

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = logStreamingStarted(streamingStarted, numberOfActiveExecutors)

  override val isDebugEnabled: Boolean = IsDebugEnabled

  def processBatch(info: BatchInfo): Boolean = {
    if (isInValidBatch(info)) return false

    val stat = statBuilder.update(info, numberOfActiveExecutors, isSLOViolated(info))
    if (stat.nonEmpty) logStat(stat.get)

    true
  }

  def isInValidBatch(info: BatchInfo): Boolean = {
    if (info.processingDelay.isEmpty) {
      logEmptyBatch(info.batchTime.milliseconds)
      return true
    }
    if (info.processingDelay.get >= MaximumLatency) {
      logExcessiveProcessingTime(info.processingDelay.get)
      return true
    }
    logBatchOK(info.batchTime.milliseconds)
    false
  }

  def isSLOViolated(info: BatchInfo): Boolean = info.processingDelay.get.toInt >= TargetLatency
}
