package com.sap.rl

import com.sap.rl.rm.RMConstants._
import org.apache.log4j.BasicConfigurator
import org.apache.spark.SparkConf

object TestCommons {

  // configure log4j
  BasicConfigurator.configure()

  def generateSparkConf(): SparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set(CoresPerTaskKey, "1")
    .set(CoresPerExecutorKey, "1")
    .set(MinimumExecutorsKey, "5")
    .set(MaximumExecutorsKey, "25")
    .set(LatencyGranularityKey, "20")
    .set(MinimumLatencyKey, "300")
    .set(TargetLatencyKey, "800")
    .set(MaximumLatencyKey, "10000")
    .set(MaximumIncomingMessagesKey, "10000")
    .set(IncomingMessagesGranularityKey, "400")
}
