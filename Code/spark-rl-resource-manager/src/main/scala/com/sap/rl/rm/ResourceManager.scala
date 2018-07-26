package com.sap.rl.rm

import java.util.concurrent.atomic.AtomicLong

import com.sap.rl.rm.LogStatus._
import org.apache.log4j.Logger
import org.apache.spark.scheduler._
import org.apache.spark.streaming.scheduler._
import org.apache.spark.{SparkConf, SparkContext}

trait ResourceManager extends StreamingListener with SparkListenerTrait with ExecutorAllocator {

  protected lazy val sparkContext: SparkContext = streamingContext.sparkContext
  protected lazy val sparkConf: SparkConf = sparkContext.getConf
  protected val log: Logger
  protected val constants: RMConstants
  protected lazy val numberOfSLOViolations: AtomicLong = new AtomicLong()
  protected lazy val totalNumberOfBatches: AtomicLong = new AtomicLong()

  import constants._

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
    log.info(s"$MY_TAG -- $APP_ENDED -- (ApplicationEndTime,NumberOfBatches,SLOViolations) = (${applicationEnd.time},${totalNumberOfBatches.get()},${numberOfSLOViolations.get()})")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    totalNumberOfBatches.incrementAndGet()
  }

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    log.info(s"$MY_TAG -- $STREAMING_STARTED -- (StreamingStartTime,All,Workers,Receivers) = (${streamingStarted.time},$numberOfActiveExecutors,$numberOfWorkerExecutors,$numberOfReceiverExecutors)")
  }

  def incrementSLOViolations(): Unit = numberOfSLOViolations.getAndIncrement()

  def isSLOViolated(info: BatchInfo): Boolean = (info.processingDelay.get.toInt / LatencyGranularity) >= CoarseTargetLatency

  def logAndCountSLOInfo(info: BatchInfo): Unit = {
    // log current and last state, as well
    if (isSLOViolated(info)) {
      incrementSLOViolations()
    }

    log.info(s"$MY_TAG -- $SLO_INFO -- (NumberOfBatches,SLOViolations) = (${totalNumberOfBatches.get()},${numberOfSLOViolations.get()})")
  }
}