package org.apache.spark.streaming.scheduler

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.ExecutorAllocationManager.isDynamicAllocationEnabled
import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext, SparkException}

abstract class ResourceManager(streamingContext: StreamingContext) extends Logging {

  import ResourceManager._

  val sparkConf: SparkConf       = streamingContext.conf
  val sparkContext: SparkContext = streamingContext.sparkContext
  val batchDuration: Long        = streamingContext.graph.batchDuration.milliseconds

  val minimumExecutors: Int = sparkConf.getInt(MinimumExecutorsKey, MinimumExecutorsDefault)
  val maximumExecutors: Int = sparkConf.getInt(MaximumExecutorsKey, MaximumExecutorsDefault)
  val minimumLatency: Long = sparkConf.getTimeAsMs(MinimumLatencyKey, MinimumLatencyDefault)
  val maximumLatency: Long = sparkConf.getTimeAsMs(MaximumLatencyKey, MaximumLatencyDefault)
  val targetLatency: Long = sparkConf.getTimeAsMs(TargetLatencyKey, TargetLatencyDefault)
  val latencyGranularity: Long = sparkConf.getTimeAsMs(LatencyGranularityKey, LatencyGranularityDefault)
  val executorGranularity: Int = sparkConf.getInt(ExecutorGranularityKey, ExecutorGranularityDefault)
  val startupWaitTime: Long = sparkConf.getTimeAsMs(StartupWaitTimeKey, StartupWaitTimeDefault)
  val initialLearningPeriod: Long = sparkConf.getTimeAsMs(InitialLearningPeriodKey, InitialLearningPeriodDefault)

  // Interface for manipulating number of executors
  protected lazy val executorAllocator: ExecutorAllocationClient = sparkContext.schedulerBackend match {
    case backend: ExecutorAllocationClient => backend
    case _ =>
      throw new SparkException(
        """|Dynamic resource allocation doesn't work in local mode. Please consider using
           |scheduler extending CoarseGrainedSchedulerBackend (such as Spark Standalone,
           |YARN or Mesos).""".stripMargin
      )
  }

  validateSettings()
  logConfiguration()

  val gracePeriod: Long = sparkConf.getTimeAsMs(GracePeriodKey, GracePeriodDefault)

  // default listening infrastructure for spark-streaming
  val listener: StreamingListener = new StreamingListener {}

  def start(): Unit = log.info("Started resource manager")

  def stop(): Unit = log.info("Stopped resource manager")

  def workingExecutors: Seq[String] = executors.diff(receivers)

  def executors: Seq[String] = executorAllocator.getExecutorIds

  def receivers: Seq[String] = streamingContext.scheduler.receiverTracker.allocatedExecutors.values.flatten.toSeq

  private def validateSettings(): Unit = {
    require(isDynamicAllocationEnabled(sparkConf))
    require(maximumExecutors >= minimumExecutors)
    require(minimumExecutors > 0)
    require(executorGranularity > 0)
  }

  private def logConfiguration(): Unit = {
    log.info("minimumExecutors: {}", minimumExecutors)
    log.info("maximumExecutors: {}", maximumExecutors)
    log.info("minimumLatency: {}", minimumLatency)
    log.info("maximumLatency: {}", maximumLatency)
    log.info("targetLatency: {}", targetLatency)
    log.info("latencyGranularity: {}", latencyGranularity)
    log.info("executorGranularity: {}", executorGranularity)
    log.info("startupWaitTime: {}", startupWaitTime)
    log.info("initialLearningPeriod: {}", initialLearningPeriod)
    log.info("gracePeriod: {}", gracePeriod)
  }
}

object ResourceManager {
  final val MinimumExecutorsKey = "spark.streaming.dynamicAllocation.minExecutors"
  final val MinimumExecutorsDefault = 1

  final val MaximumExecutorsKey = "spark.streaming.dynamicAllocation.maxExecutors"
  final val MaximumExecutorsDefault = Int.MaxValue

  final val MinimumLatencyKey = "spark.streaming.dynamicAllocation.minLatency"
  final val MinimumLatencyDefault = "100ms"

  final val MaximumLatencyKey = "spark.streaming.dynamicAllocation.maxLatency"
  final val MaximumLatencyDefault = "10s"

  final val TargetLatencyKey = "spark.streaming.dynamicAllocation.targetLatency"
  final val TargetLatencyDefault = "800ms"

  final val LatencyGranularityKey = "spark.streaming.dynamicAllocation.latencyGranularity"
  final val LatencyGranularityDefault = "10ms"

  final val ExecutorGranularityKey = "spark.streaming.dynamicAllocation.executorGranularity"
  final val ExecutorGranularityDefault = 1

  final val StartupWaitTimeKey = "spark.streaming.dynamicAllocation.startupWaitTime"
  final val StartupWaitTimeDefault = "30s"

  final val InitialLearningPeriodKey = "spark.streaming.dynamicAllocation.initialLearningPeriod"
  final val InitialLearningPeriodDefault = "60s"

  final val GracePeriodKey = "spark.streaming.dynamicAllocation.gracePeriodKey"
  final val GracePeriodDefault = "60s"
}