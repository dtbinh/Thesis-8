package com.sap.rm

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.apache.spark.streaming.scheduler._

import scala.util.Random.shuffle

trait ResourceManager extends Spark with StreamingListener with SparkListenerTrait {

  protected val config: ResourceManagerConfig
  import config._

  private lazy val statBuilder: StatBuilder = StatBuilder(ReportDuration)
  protected var streamingStartTime: Long = 0
  private var batchCount: Int = 0
  private var startup = true

  @transient protected lazy val logger = ResourceManagerLogger(config)
  import logger._

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = logExecutorAdded(executorAdded)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = logExecutorRemoved(executorRemoved)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = logApplicationEnd(applicationEnd)

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    streamingStartTime = streamingStarted.time
    logStreamingStarted(streamingStarted, numberOfActiveExecutors)
  }

  def processBatch(info: BatchInfo): Boolean = {
    incrementCorrelationId()

    if (startup && numberOfActiveExecutors == MaximumExecutors) {
      startup = false
      val diff = MaximumExecutors - MinimumExecutors
      if (diff > 0) {
        removeExecutors(shuffle(activeExecutors).take(MaximumExecutors - MinimumExecutors))
      }
    }

    if (isInvalidBatch(info)) return false

    val stat = statBuilder.update(info, numberOfActiveExecutors)
    if (stat.nonEmpty) logStat(stat.get)

    true
  }

  def isInvalidBatch(info: BatchInfo): Boolean = {
    batchCount += 1
    if (batchCount <= StartupIgnoreBatches) {
      logStartupIgnoreBatch(info.batchTime.milliseconds)
      return true
    }
    if (info.totalDelay.isEmpty || info.numRecords == 0) {
      logEmptyBatch(info.batchTime.milliseconds)
      return true
    }
    if (info.totalDelay.get >= MaximumLatency) {
      logExcessiveProcessingTime(info.totalDelay.get)
      return true
    }
    false
  }
}
