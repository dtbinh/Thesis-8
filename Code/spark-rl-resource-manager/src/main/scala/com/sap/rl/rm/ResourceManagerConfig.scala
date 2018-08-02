package com.sap.rl.rm

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import ResourceManagerConfig._

class ResourceManagerConfig(sparkConf: SparkConf) {

  @transient lazy val log: Logger = Logger(classOf[ResourceManagerConfig])

  final val CoresPerTask: Int = sparkConf.getInt(CoresPerTaskKey, CoresPerTaskDefault)
  final val CoresPerExecutor: Int = sparkConf.getInt(CoresPerExecutorKey, CoresPerExecutorDefault)
  final val ExecutorChangePerStep: Int = sparkConf.getInt(ExecutorChangePerStepKey, ExecutorChangePerStepDefault)
  final val MinimumExecutors: Int = sparkConf.getInt(MinimumExecutorsKey, MinimumExecutorsDefault)
  final val MaximumExecutors: Int = sparkConf.getInt(MaximumExecutorsKey, MaximumExecutorsDefault)
  final val MinimumLatency: Int = sparkConf.getTimeAsMs(MinimumLatencyKey, MinimumLatencyDefault).toInt
  final val MaximumLatency: Int = sparkConf.getTimeAsMs(MaximumLatencyKey, MaximumLatencyDefault).toInt
  final val TargetLatency: Int = sparkConf.getTimeAsMs(TargetLatencyKey, TargetLatencyDefault).toInt
  final val LatencyGranularity: Int = sparkConf.getTimeAsMs(LatencyGranularityKey, LatencyGranularityDefault).toInt
  final val GracePeriod: Int = sparkConf.getTimeAsMs(GracePeriodKey, GracePeriodDefault).toInt
  final val WindowSize: Int = sparkConf.getInt(WindowSizeKey, WindowSizeDefault)
  final val LearningFactor: Double = sparkConf.getDouble(LearningFactorKey, LearningFactorDefault)
  final val DiscountFactor: Double = sparkConf.getDouble(DiscountFactorKey, DiscountFactorDefault)
  final val CoarseMinimumLatency: Int = MinimumLatency / LatencyGranularity
  final val CoarseTargetLatency: Int = TargetLatency / LatencyGranularity
  final val CoarseMaximumLatency: Int = MaximumLatency / LatencyGranularity
  final val BestReward: Double = sparkConf.getDouble(BestRewardKey, BestRewardDefault)
  final val NoReward: Double = sparkConf.getDouble(NoRewardKey, NoRewardDefault)
  final val IsDebugEnabled = sparkConf.getBoolean(IsDebugEnabledKey, IsDebugEnabledDefault)
  final val InitializationMode = sparkConf.get(InitializationModeKey, InitializationModeDefault)
  final val ReportDuration = sparkConf.getTimeAsMs(ReportDurationKey, ReportDurationDefault)

  require(CoresPerExecutor == CoresPerTask)
  require(ExecutorChangePerStep >= 0)
  require(ExecutorChangePerStep <= MaximumExecutors)

  require(MaximumExecutors >= MinimumExecutors)
  require(MinimumExecutors > 0)
  require(WindowSize > 0)

  require(LearningFactor >= 0 && LearningFactor <= 1)
  require(DiscountFactor >= 0 && DiscountFactor <= 1)

  require(BestReward > NoReward)

  val config: String =
    s""" --- Configuration ---
       | CoresPerExecutor: $CoresPerExecutor
       | CoresPerTask: $CoresPerTask
       | ExecutorChangePerStep: $ExecutorChangePerStep
       | MinimumExecutors: $MinimumExecutors
       | MaximumExecutors: $MaximumExecutors
       | MinimumLatency: $MinimumLatency
       | MaximumLatency: $MaximumLatency
       | TargetLatency: $TargetLatency
       | LatencyGranularity: $LatencyGranularity
       | GracePeriod: $GracePeriod
       | WindowSize: $WindowSize
       | LearningFactor: $LearningFactor
       | DiscountFactor: $DiscountFactor
       | CoarseMinimumLatency: $CoarseMinimumLatency
       | CoarseTargetLatency: $CoarseTargetLatency
       | CoarseMaximumLatency: $CoarseMaximumLatency
       | BestReward: $BestReward
       | NoReward: $NoReward
       | IsDebugEnabled: $IsDebugEnabled
       | InitializationMode: $InitializationMode
       | ReportDuration: $ReportDuration
       | --- Configuration ---""".stripMargin

  log.info(config)
}

object ResourceManagerConfig {

  final val LessThan: Int = -1
  final val GreaterThan: Int = 1
  final val Equal: Int = 0

  final val CoresPerTaskKey = "spark.task.cpus"
  final val CoresPerTaskDefault = 1
  final val CoresPerExecutorKey = "spark.executor.cores"
  final val CoresPerExecutorDefault = 0
  final val MinimumExecutorsKey = "spark.streaming.dynamicAllocation.minExecutors"
  final val MinimumExecutorsDefault = 1
  final val MaximumExecutorsKey = "spark.streaming.dynamicAllocation.maxExecutors"
  final val MaximumExecutorsDefault = Int.MaxValue
  final val ExecutorChangePerStepKey = "spark.streaming.dynamicAllocation.executorChangePerStep"
  final val ExecutorChangePerStepDefault = 2
  final val MinimumLatencyKey = "spark.streaming.dynamicAllocation.minLatency"
  final val MinimumLatencyDefault = "100ms"
  final val MaximumLatencyKey = "spark.streaming.dynamicAllocation.maxLatency"
  final val MaximumLatencyDefault = "10s"
  final val TargetLatencyKey = "spark.streaming.dynamicAllocation.targetLatency"
  final val TargetLatencyDefault = "800ms"
  final val LatencyGranularityKey = "spark.streaming.dynamicAllocation.latencyGranularity"
  final val LatencyGranularityDefault = "10ms"
  final val GracePeriodKey = "spark.streaming.dynamicAllocation.gracePeriod"
  final val GracePeriodDefault = "60s"
  final val WindowSizeKey = "spark.streaming.dynamicAllocation.windowSize"
  final val WindowSizeDefault = 10
  final val LearningFactorKey = "spark.streaming.dynamicAllocation.learningFactor"
  final val LearningFactorDefault = 0.7
  final val DiscountFactorKey = "spark.streaming.dynamicAllocation.discountFactor"
  final val DiscountFactorDefault = 0.9
  final val BestRewardKey = "spark.streaming.dynamicAllocation.bestReward"
  final val BestRewardDefault = 1.0
  final val NoRewardKey = "spark.streaming.dynamicAllocation.noReward"
  final val NoRewardDefault = 0
  final val IsDebugEnabledKey = "spark.streaming.dynamicAllocation.isDebugEnabled"
  final val IsDebugEnabledDefault = true
  final val InitializationModeKey = "spark.streaming.dynamicAllocation.initializationMode"
  final val InitializationModeDefault = "optimal" // zero, random
  final val ReportDurationKey = "spark.streaming.dynamicAllocation.reportDuration"
  final val ReportDurationDefault = "3m"

  def apply(sparkConf: SparkConf): ResourceManagerConfig = new ResourceManagerConfig(sparkConf)
}