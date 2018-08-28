package com.sap.rm

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.apache.spark.streaming.scheduler._
import scala.util.Random.shuffle

trait ResourceManager extends Spark with StreamingListener with SparkListenerTrait with ResourceManagerLogger {

  protected val config: ResourceManagerConfig
  import config._

  private lazy val statBuilder: StatBuilder = StatBuilder(ReportDuration)
  protected var streamingStartTime: Long = 0

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = logExecutorAdded(executorAdded)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = logExecutorRemoved(executorRemoved)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = logApplicationEnd(applicationEnd)

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    streamingStartTime = streamingStarted.time
    logStreamingStarted(streamingStarted, numberOfActiveExecutors)
    removeExecutors(shuffle(activeExecutors).take(MaximumExecutors - MinimumExecutors))
  }

  override val isDebugEnabled: Boolean = IsDebugEnabled

  private var batchCount: Int = 0

  def processBatch(info: BatchInfo): Boolean = {
    if (isInvalidBatch(info)) return false

    val stat = statBuilder.update(info, numberOfActiveExecutors, isSLOViolated(info))
    if (stat.nonEmpty) logStat(stat.get)

    true
  }

  def isInvalidBatch(info: BatchInfo): Boolean = {
    batchCount += 1
    if (batchCount <= StartupIgnoreBatches) {
      logStartupIgnoreBatch(info.batchTime.milliseconds)
      return true
    }
    if (info.processingDelay.isEmpty || info.totalDelay.isEmpty || info.numRecords == 0) {
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
