package com.sap.rl

import com.sap.rl.rm.{ResourceManager, ResourceManagerConfig}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

class DoNothingResourceManager(val config: ResourceManagerConfig, val streamingContext: StreamingContext) extends ResourceManager {
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    processBatch(batchCompleted.batchInfo)
  }

  override def numberOfActiveExecutors: Int = config.MaximumExecutors
}
