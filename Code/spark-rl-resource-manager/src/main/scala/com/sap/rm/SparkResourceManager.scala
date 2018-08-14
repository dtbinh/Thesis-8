package com.sap.rm

import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListenerStreamingStarted}

class SparkResourceManager(val config: ResourceManagerConfig, val streamingContext: StreamingContext) extends ResourceManager {

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    sparkDynamicAllocator.start()
    super.onStreamingStarted(streamingStarted)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(applicationEnd)
    sparkDynamicAllocator.stop()
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    if (processBatch(batchCompleted.batchInfo)) {
      sparkDynamicAllocator.onBatchCompleted(batchCompleted)
    }
  }
}

object SparkResourceManager {
  def apply(config: ResourceManagerConfig, ssc: StreamingContext): SparkResourceManager = {
    new SparkResourceManager(config, ssc)
  }
}
