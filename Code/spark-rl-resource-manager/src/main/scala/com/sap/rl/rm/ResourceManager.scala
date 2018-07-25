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

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info(s"$APP_ENDED -- (ApplicationEndTime,NumberOfBatches,SLOViolations) = (${applicationEnd.time},${totalNumberOfBatches.get()},${numberOfSLOViolations.get()})")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    totalNumberOfBatches.incrementAndGet()
  }

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    log.info(s"$STREAMING_STARTED -- StreamingStartTime = ${streamingStarted.time}")
  }

  def incrementSLOViolations(): Unit = numberOfSLOViolations.getAndIncrement()

  def isSLOViolated(info: BatchInfo): Boolean = (info.processingDelay.get.toInt / LatencyGranularity) >= CoarseTargetLatency

  def logAndCountSLOInfo(info: BatchInfo): Unit = {
    // log current and last state, as well
    if (isSLOViolated(info)) {
      incrementSLOViolations()
    }

    log.info(s"$SLO_INFO -- (NumberOfBatches,SLOViolations) = (${totalNumberOfBatches.get()},${numberOfSLOViolations.get()})")
  }
}