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
  protected var streamingStartTime: Long = 0
  protected lazy val numberOfSLOViolations: AtomicLong = new AtomicLong(0)

  import constants._

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    log.warn(s"$SPARK_EXEC_ADDED -- (ID,Time) = (${executorAdded.executorId},${executorAdded.time}")
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    log.warn(s"$SPARK_EXEC_REMOVED -- (ID,Time) = (${executorRemoved.executorId},${executorRemoved.time}")
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    log.info(s"$APP_STARTED -- ApplicationStartTime = ${applicationStart.time}")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info(s"$APP_ENDED -- (ApplicationEndTime,SLOViolations) = (${applicationEnd.time},${numberOfSLOViolations.get()})")
  }

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    streamingStartTime = streamingStarted.time
    log.info(s"$STREAMING_STARTED -- StreamingStartTime = $streamingStartTime")
  }

  def incrementSLOViolations(): Unit = numberOfSLOViolations.getAndIncrement()

  def isSLOViolated(info: BatchInfo): Boolean = info.processingDelay.get.toInt >= CoarseTargetLatency
}