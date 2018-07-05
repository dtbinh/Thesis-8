package org.apache.spark.streaming

import com.sap.rl.rm.ResourceManager
import org.apache.spark.streaming.scheduler.ExecutorAllocationManager.isDynamicAllocationEnabled
import org.apache.spark.{SparkConf, SparkContext}

class RLStreamingContext(resourceManager: Option[ResourceManager], sparkContext: SparkContext, checkpoint: Checkpoint, batchDuration: Duration)
  extends StreamingContext(sparkContext, checkpoint, batchDuration) {

  import RLStreamingContext._

  private val sparkConf: SparkConf = sparkContext.getConf

  override def start(): Unit = {
    if (sparkConf.getBoolean(RLDynamicResourceAllocationEnabledKey, RLDynamicResourceAllocationEnabledDefault)) {
      // do not start default ResourceAllocationManager
      conf.set(DynamicResourceAllocationEnabledKey, "false")
      super.start()

      // enable dynamic resource allocation again
      conf.set(DynamicResourceAllocationEnabledKey, "true")
      require(isDynamicAllocationEnabled(sparkConf))

      // start our own resource manager
      addStreamingListener(resourceManager.get)
      resourceManager.get.start()
    } else {
      super.start()
    }
  }

  override def stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit = {
    if (resourceManager.nonEmpty) resourceManager.get.stop()
    super.stop(stopSparkContext, stopGracefully)
  }
}

object RLStreamingContext {
  final val DynamicResourceAllocationEnabledKey = "spark.streaming.dynamicAllocation.enabled"
  final val DynamicResourceAllocationEnabledDefault = false
  final val RLDynamicResourceAllocationEnabledKey = "spark.streaming.RLDynamicAllocation.enabled"
  final val RLDynamicResourceAllocationEnabledDefault = false
}