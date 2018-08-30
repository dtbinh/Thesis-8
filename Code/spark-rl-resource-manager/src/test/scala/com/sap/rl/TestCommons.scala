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
    .set(MinimumExecutorsKey, "5")
    .set(MaximumExecutorsKey, "25")
    .set(LatencyGranularityKey, "100")
    .set(TargetLatencyKey, "800")
    .set(MaximumLatencyKey, "10000")

  def createConfig(sparkConf: SparkConf): ResourceManagerConfig = ResourceManagerConfig(sparkConf)
}
