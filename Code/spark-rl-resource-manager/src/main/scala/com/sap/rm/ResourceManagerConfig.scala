package com.sap.rm

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf

class ResourceManagerConfig(sparkConf: SparkConf) {

  @transient private[ResourceManagerConfig] lazy val log: Logger = Logger("RMLogs")
  import ResourceManagerConfig._

  final val CoresPerTask: Int = sparkConf.getInt(CoresPerTaskKey, CoresPerTaskDefault)
  final val CoresPerExecutor: Int = sparkConf.getInt(CoresPerExecutorKey, CoresPerExecutorDefault)
  final val ExecutorGranularity: Int = sparkConf.getInt(ExecutorGranularityKey, ExecutorGranularityDefault)
  final val IncomingMessageGranularity: Int = sparkConf.getInt(IncomingMessageGranularityKey, IncomingMessageGranularityDefault)
  final val MinimumExecutors: Int = sparkConf.getInt(MinimumExecutorsKey, MinimumExecutorsDefault)
  final val MaximumExecutors: Int = sparkConf.getInt(MaximumExecutorsKey, MaximumExecutorsDefault)
  final val MaximumLatency: Int = sparkConf.getTimeAsMs(MaximumLatencyKey, MaximumLatencyDefault).toInt
  final val TargetLatency: Int = sparkConf.getTimeAsMs(TargetLatencyKey, TargetLatencyDefault).toInt
  final val LatencyGranularity: Int = sparkConf.getTimeAsMs(LatencyGranularityKey, LatencyGranularityDefault).toInt
  final val WindowSize: Int = sparkConf.getInt(WindowSizeKey, WindowSizeDefault)
  final val LearningFactor: Double = sparkConf.getDouble(LearningFactorKey, LearningFactorDefault)
  final val DiscountFactor: Double = sparkConf.getDouble(DiscountFactorKey, DiscountFactorDefault)
  final val CoarseTargetLatency: Int = TargetLatency / LatencyGranularity
  final val CoarseMaximumLatency: Int = MaximumLatency / LatencyGranularity
  final val BestReward: Double = sparkConf.getDouble(BestRewardKey, BestRewardDefault)
  final val NoReward: Double = sparkConf.getDouble(NoRewardKey, NoRewardDefault)
  final val IsDebugEnabled: Boolean = sparkConf.getBoolean(IsDebugEnabledKey, IsDebugEnabledDefault)
  final val InitializationMode: String = sparkConf.get(InitializationModeKey, InitializationModeDefault)
  final val ReportDuration: Long = sparkConf.getTimeAsMs(ReportDurationKey, ReportDurationDefault)
  final val StartupIgnoreBatches: Int = sparkConf.getInt(StartupIgnoreBatchesKey, StartupIgnoreBatchesDefault)
  final val Epsilon: Double = sparkConf.getDouble(EpsilonKey, EpsilonDefault)
  final val EpsilonStep: Double = sparkConf.getDouble(EpsilonStepKey, EpsilonStepDefault)
  final val ValueIterationInitializationTime: Long = sparkConf.getTimeAsMs(ValueIterationInitializationTimeKey, ValueIterationInitializationTimeDefault)
  final val ValueIterationInitializationCount: Int = sparkConf.getInt(ValueIterationInitializationCountKey, ValueIterationInitializationCountDefault)
  final val Policy: String = sparkConf.get(PolicyKey, PolicyDefault)
  final val Reward: String = sparkConf.get(RewardKey, RewardDefault)
  final val DecisionInterval: Int = sparkConf.getTimeAsMs(DecisionIntervalKey, DecisionIntervalDefault).toInt
  final val ExecutorStrategy: String = sparkConf.get(ExecutorStrategyKey, ExecutorStrategyDefault)

  require(CoresPerExecutor == CoresPerTask)
  require(ExecutorGranularity >= 0)
  require(ExecutorGranularity <= MaximumExecutors)

  require(MaximumExecutors >= MinimumExecutors)
  require(MinimumExecutors > 0)
  require(WindowSize > 0)

  require(LearningFactor >= 0 && LearningFactor <= 1)
  require(DiscountFactor >= 0 && DiscountFactor <= 1)

  require(BestReward > NoReward)
  require(StartupIgnoreBatches >= 0)
  require(Epsilon >= 0 && Epsilon <= 1)
  require(EpsilonStep >= 0 && EpsilonStep <= 1)
  require(ValueIterationInitializationTime > 0)
  require(ValueIterationInitializationCount > 0)
  require(Policy == "greedy" || Policy == "oneMinusEpsilon" || Policy == "decreasingOneMinusEpsilon")
  require(Reward == "preferScaleInWhenLoadIsDescreasing" || Reward == "preferNoActionWhenLoadIsDecreasing")
  require(ExecutorStrategy == "static" || ExecutorStrategy == "linear" || ExecutorStrategy == "relative")

  val config: String =
    s""" --- Configuration ---
       | CoresPerExecutor: $CoresPerExecutor
       | CoresPerTask: $CoresPerTask
       | ExecutorGranularity: $ExecutorGranularity
       | MinimumExecutors: $MinimumExecutors
       | MaximumExecutors: $MaximumExecutors
       | MaximumLatency: $MaximumLatency
       | TargetLatency: $TargetLatency
       | LatencyGranularity: $LatencyGranularity
       | WindowSize: $WindowSize
       | LearningFactor: $LearningFactor
       | DiscountFactor: $DiscountFactor
       | CoarseTargetLatency: $CoarseTargetLatency
       | CoarseMaximumLatency: $CoarseMaximumLatency
       | BestReward: $BestReward
       | NoReward: $NoReward
       | IsDebugEnabled: $IsDebugEnabled
       | InitializationMode: $InitializationMode
       | ReportDuration: $ReportDuration
       | StartupIgnoreBatches: $StartupIgnoreBatches
       | Epsilon: $Epsilon
       | EpsilonStep: $EpsilonStep
       | ValueIterationInitializationTime: $ValueIterationInitializationTime
       | ValueIterationInitializationCount: $ValueIterationInitializationCount
       | Policy: $Policy
       | Reward: $Reward
       | DecisionInterval: $DecisionInterval
       | ExecutorStrategy: $ExecutorStrategy
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
  final val ExecutorGranularityKey = "spark.streaming.dynamicAllocation.executorGranularity"
  final val ExecutorGranularityDefault = 2
  final val IncomingMessageGranularityKey = "spark.streaming.dynamicAllocation.incomingMessageGranularity"
  final val IncomingMessageGranularityDefault = 50
  final val MaximumLatencyKey = "spark.streaming.dynamicAllocation.maxLatency"
  final val MaximumLatencyDefault = "10s"
  final val TargetLatencyKey = "spark.streaming.dynamicAllocation.targetLatency"
  final val TargetLatencyDefault = "800ms"
  final val LatencyGranularityKey = "spark.streaming.dynamicAllocation.latencyGranularity"
  final val LatencyGranularityDefault = "10ms"
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
  final val StartupIgnoreBatchesKey = "spark.streaming.dynamicAllocation.startupIgnoreBatches"
  final val StartupIgnoreBatchesDefault = 3
  final val EpsilonKey = "spark.streaming.dynamicAllocation.epsilon"
  final val EpsilonDefault = 0.1
  final val EpsilonStepKey = "spark.streaming.dynamicAllocation.epsilonStep"
  final val EpsilonStepDefault = 0.05
  final val ValueIterationInitializationTimeKey = "spark.streaming.dynamicAllocation.valueIterationInitializationTime"
  final val ValueIterationInitializationTimeDefault = "15m"
  final val ValueIterationInitializationCountKey = "spark.streaming.dynamicAllocation.valueIterationInitializationCount"
  final val ValueIterationInitializationCountDefault = 10
  final val PolicyKey = "spark.streaming.dynamicAllocation.policy"
  final val PolicyDefault = "greedy"
  final val RewardKey = "spark.streaming.dynamicAllocation.reward"
  final val RewardDefault = "preferScaleInWhenLoadIsDescreasing" // or preferNoActionWhenLoadIsDecreasing
  final val DecisionIntervalKey = "spark.streaming.dynamicAllocation.decisionInterval"
  final val DecisionIntervalDefault = "60s"
  final val ExecutorStrategyKey = "spark.streaming.dynamicAllocation.executorStrategy"
  final val ExecutorStrategyDefault = "static" // linear

  def apply(sparkConf: SparkConf): ResourceManagerConfig = new ResourceManagerConfig(sparkConf)
}