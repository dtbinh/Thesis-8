package com.sap.rl.rm

import com.sap.rl.rm.LogStatus._
import org.apache.log4j.Logger
import org.apache.spark.scheduler._
import org.apache.spark.streaming.scheduler._
import org.apache.spark.{SparkConf, SparkContext}

trait ResourceManager extends StreamingListener with SparkListenerTrait with ExecutorAllocator {

  private val REPORT_DURATION: Int = 5 * 60 * 1000

  protected lazy val sparkContext: SparkContext = streamingContext.sparkContext
  protected lazy val sparkConf: SparkConf = sparkContext.getConf
  protected val log: Logger
  protected val constants: RMConstants
  import constants._

  private var statTotalSLOViolations: Int = 0
  private var statTotalBatches: Int = 0

  private var statSumLatency: Int = 0
  private var statMinLatency: Int = Int.MaxValue
  private var statMaxLatency: Int = Int.MinValue

  private var statSumExecutor: Int = 0
  private var statMinExecutor: Int = Int.MaxValue
  private var statMaxExecutor: Int = Int.MinValue

  private var windowCounter: Int = 0
  private var statWindowBatches: Int = 0
  private var statWindowSLOViolations: Int = 0
  private var lastTimeReportLogged: Long = 0

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    log.info(s"$MY_TAG -- $APP_STARTED -- ApplicationStartTime = ${applicationStart.time}")
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    log.info(s"$MY_TAG -- $SPARK_EXEC_ADDED -- (ID,Time,All,Workers,Receivers) = (${executorAdded.executorId},${executorAdded.time},$numberOfActiveExecutors,$numberOfWorkerExecutors,$numberOfReceiverExecutors)")
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    log.info(s"$MY_TAG -- $SPARK_EXEC_REMOVED -- (ID,Time,All,Workers,Receivers) = (${executorRemoved.executorId},${executorRemoved.time},$numberOfActiveExecutors,$numberOfWorkerExecutors,$numberOfReceiverExecutors)")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info(s"$MY_TAG -- $APP_ENDED -- ApplicationEndTime = ${applicationEnd.time}")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val info = batchCompleted.batchInfo
    if (lastTimeReportLogged == 0) {
      lastTimeReportLogged = info.batchTime.milliseconds
      return
    }

    statTotalBatches += 1
    statWindowBatches += 1
    if (isSLOViolated(info)) {
      statTotalSLOViolations += 1
      statWindowSLOViolations += 1
    }

    statSumLatency += info.processingDelay.get.toInt
    statMinLatency = Math.min(statMinLatency, info.processingDelay.get.toInt)
    statMaxLatency = Math.max(statMaxLatency, info.processingDelay.get.toInt)

    val currentWorkers = numberOfWorkerExecutors
    statSumExecutor += currentWorkers
    statMinExecutor = Math.min(statMinExecutor, currentWorkers)
    statMaxExecutor = Math.max(statMaxExecutor, currentWorkers)

    if ((info.batchTime.milliseconds - lastTimeReportLogged) >= REPORT_DURATION) {
      windowCounter += 1

      log.info(
        s""" --- $MY_TAG -- $STAT ---
           | ==========================
           | TotalBatches=$statTotalBatches
           | CurrentWindow: $windowCounter
           | ==========================
           | AverageLatency=${statSumLatency.toDouble / statWindowBatches}
           | MinimumLatency=$statMinLatency
           | MaximumLatency=$statMaxLatency
           | ==========================
           | AverageExecutors=${statSumExecutor / statWindowBatches}
           | MinimumExecutors=$statMinExecutor
           | MaximumExecutors=$statMaxExecutor
           | ==========================
           | TotalSLOViolations=$statTotalSLOViolations
           | WindowSLOViolations=$statWindowSLOViolations
           | ==========================
           | --- $MY_TAG -- $STAT ---""".stripMargin)

      // reset stats
      statWindowBatches = 0
      statWindowSLOViolations = 0

      statSumLatency = 0
      statMinLatency = Int.MaxValue
      statMaxLatency = Int.MinValue

      statSumExecutor = 0
      statMinExecutor = Int.MaxValue
      statMaxExecutor = Int.MinValue

      lastTimeReportLogged = info.batchTime.milliseconds
    }
  }

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    log.info(s"$MY_TAG -- $STREAMING_STARTED -- (StreamingStartTime,All,Workers,Receivers) = (${streamingStarted.time},$numberOfActiveExecutors,$numberOfWorkerExecutors,$numberOfReceiverExecutors)")
  }

  def isSLOViolated(info: BatchInfo): Boolean = info.processingDelay.get.toInt >= TargetLatency
}