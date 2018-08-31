package com.sap.rl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.ResourceManagerConfig._
import org.apache.log4j.BasicConfigurator
import org.apache.spark.SparkConf

object TestCommons {

  // configure log4j
  BasicConfigurator.configure()

  def createSparkConf(): SparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set(CoresPerTaskKey, "1")
    .set(CoresPerExecutorKey, "1")
    .set(MinimumExecutorsKey, "4")
    .set(MaximumExecutorsKey, "20")
    .set(ExecutorGranularityKey, "2")
    .set(LatencyGranularityKey, "400")
    .set(TargetLatencyKey, "4500")
    .set(MaximumLatencyKey, "30000")
    .set(IncomingMessageGranularityKey, "100")
    .set(PolicyKey, "greedy")
    .set(InitializationModeKey, "optimal")
    .set(RewardKey, "preferScaleInWhenLoadIsDescreasing")

  def createConfig(sparkConf: SparkConf): ResourceManagerConfig = ResourceManagerConfig(sparkConf)
}
