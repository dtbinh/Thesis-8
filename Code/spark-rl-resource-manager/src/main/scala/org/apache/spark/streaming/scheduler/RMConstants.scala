package org.apache.spark.streaming.scheduler

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler.ExecutorAllocationManager.isDynamicAllocationEnabled

class RMConstants(sparkConf: SparkConf) extends Logging {

  import RMConstants._

  final val CoresPerTask: Int = sparkConf.getInt(CoresPerTaskKey, CoresPerTaskDefault)
  final val CoresPerExecutor: Int = sparkConf.getInt(CoresPerExecutorKey, CoresPerExecutorDefault)
  final val BackupExecutors: Int = sparkConf.getInt(BackupExecutorsKey, BackupExecutorsDefault)

  final val MinimumExecutors: Int = sparkConf.getInt(MinimumExecutorsKey, MinimumExecutorsDefault)
  final val MaximumExecutors: Int = sparkConf.getInt(MaximumExecutorsKey, MaximumExecutorsDefault)

  final val MinimumLatency: Int = sparkConf.getTimeAsMs(MinimumLatencyKey, MinimumLatencyDefault).toInt
  final val MaximumLatency: Int = sparkConf.getTimeAsMs(MaximumLatencyKey, MaximumLatencyDefault).toInt
  final val TargetLatency: Int = sparkConf.getTimeAsMs(TargetLatencyKey, TargetLatencyDefault).toInt
  final val LatencyGranularity: Int = sparkConf.getTimeAsMs(LatencyGranularityKey, LatencyGranularityDefault).toInt

  final val ExecutorGranularity: Int = sparkConf.getInt(ExecutorGranularityKey, ExecutorGranularityDefault).toInt

  final val StartupWaitTime: Int = sparkConf.getTimeAsMs(StartupWaitTimeKey, StartupWaitTimeDefault).toInt
  final val GracePeriod: Int = sparkConf.getTimeAsMs(GracePeriodKey, GracePeriodDefault).toInt

  final val WindowSize: Int = sparkConf.getInt(WindowSizeKey, WindowSizeDefault)
  final val LearningFactor: Double = sparkConf.getDouble(LearningFactorKey, LearningFactorDefault)
  final val DiscountFactor: Double = sparkConf.getDouble(DiscountFactorKey, DiscountFactorDefault)

  final val CoarseMinimumLatency: Int = MinimumLatency / LatencyGranularity
  final val CoarseTargetLatency: Int = TargetLatency / LatencyGranularity
  final val CoarseMaximumLatency: Int = MaximumLatency / LatencyGranularity

  final val BestReward: Double = sparkConf.getDouble(BestRewardKey, BestRewardDefault)
  final val NoReward: Double = sparkConf.getDouble(NoRewardKey, NoRewardDefault)

  validateSettings()
  logConfiguration()

  private def validateSettings(): Unit = {
    require(CoresPerExecutor == CoresPerTask)
    require(BackupExecutors >= 0)

    require(isDynamicAllocationEnabled(sparkConf))

    require(MaximumExecutors >= MinimumExecutors)
    require(MinimumExecutors > 0)
    require(ExecutorGranularity > 0)
    require(WindowSize > 0)

    require(LearningFactor >= 0 && LearningFactor <= 1)
    require(DiscountFactor >= 0 && DiscountFactor <= 1)

    require(BestReward > NoReward)
  }

  private def logConfiguration(): Unit = {
    val config: String =
      """ --- Configuration ---
        | CoresPerExecutor: ${CoresPerExecutor}
        | CoresPerTask: ${CoresPerTask}
        | BackupExecutors: ${BackupExecutors}
        | MinimumExecutors: ${MinimumExecutors}
        | MaximumExecutors: ${MaximumExecutors}
        | MinimumLatency: ${MinimumLatency}
        | MaximumLatency: ${MaximumLatency}
        | TargetLatency: ${TargetLatency}
        | LatencyGranularity: ${LatencyGranularity}
        | ExecutorGranularity: ${ExecutorGranularity}
        | StartupWaitTime: ${StartupWaitTime}
        | GracePeriod: ${GracePeriod}
        | WindowSize: ${WindowSize}
        | LearningFactor: ${LearningFactor}
        | DiscountFactor: ${DiscountFactor}
        | CoarseMinimumLatency: ${CoarseMinimumLatency}
        | CoarseTargetLatency: ${CoarseTargetLatency}
        | CoarseMaximumLatency: ${CoarseMaximumLatency}
        | BestReward: ${BestReward}
        | NoReward: ${NoReward}
        | --- Configuration ---
      """.stripMargin

    log.info(config)
  }
}

object RMConstants {

  def apply(sparkConf: SparkConf): RMConstants = new RMConstants(sparkConf)

  final val CoresPerTaskKey = "spark.task.cpus"
  final val CoresPerTaskDefault = 1

  final val CoresPerExecutorKey = "spark.executor.cores"
  final val CoresPerExecutorDefault = 0

  final val BackupExecutorsKey = "spark.streaming.dynamicAllocation.backupExecutors"
  final val BackupExecutorsDefault = 0

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
  final val StartupWaitTimeDefault = "60s"

  final val GracePeriodKey = "spark.streaming.dynamicAllocation.gracePeriodKey"
  final val GracePeriodDefault = "60s"

  final val WindowSizeKey = "spark.streaming.dynamicAllocation.windowSize"
  final val WindowSizeDefault = 30

  final val LearningFactorKey = "spark.streaming.dynamicAllocation.learningFactor"
  final val LearningFactorDefault = 0.5

  final val DiscountFactorKey = "spark.streaming.dynamicAllocation.discountFactor"
  final val DiscountFactorDefault = 0.9

  final val BestRewardKey = "spark.streaming.dynamicAllocation.bestReward"
  final val BestRewardDefault = 1.0

  final val NoRewardKey = "spark.streaming.dynamicAllocation.noReward"
  final val NoRewardDefault = 0

}
