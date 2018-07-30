package com.sap.rl.rm

import org.apache.spark.scheduler._
import org.apache.spark.streaming.scheduler._

import com.sap.rl.{ResourceManagerLogger, StatBuilder}

trait ResourceManager extends Spark with StreamingListener with SparkListenerTrait with ExecutorAllocator with ResourceManagerLogger {

  private lazy val statBuilder: StatBuilder = StatBuilder()

  protected val config: ResourceManagerConfig
  import config._

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = logApplicationStarted(applicationStart)

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = logExecutorAdded(executorAdded, numberOfActiveExecutors)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = logExecutorRemoved(executorRemoved, numberOfActiveExecutors)

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
    if (info.numRecords >= MaximumIncomingMessages) {
      logExcessiveIncomingMessages(info.numRecords.toInt)
      return true
    }
    logBatchOK(info.batchTime.milliseconds)
    false
  }

  def isSLOViolated(info: BatchInfo): Boolean = info.processingDelay.get.toInt >= TargetLatency
}