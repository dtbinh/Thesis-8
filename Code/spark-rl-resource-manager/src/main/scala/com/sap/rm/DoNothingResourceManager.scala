package com.sap.rm

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

class DoNothingResourceManager(val config: ResourceManagerConfig, val streamingContext: StreamingContext) extends ResourceManager {
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    processBatch(batchCompleted.batchInfo)
  }

  override def numberOfActiveExecutors: Int = config.MaximumExecutors
}

object DoNothingResourceManager {
  def apply(config: ResourceManagerConfig, ssc: StreamingContext): DoNothingResourceManager = new DoNothingResourceManager(config, ssc)
}
