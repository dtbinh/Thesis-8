package com.sap.rl.rm.vi

import com.sap.rl.rm.{RLResourceManager, ResourceManagerConfig}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

class ValueIterationResourceManager(val config: ResourceManagerConfig, val streamingContext: StreamingContext) extends RLResourceManager {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    super.onBatchCompleted(batchCompleted)
  }

  def specialize(): Unit = {
    // TODO: implement for VI
  }
}

object ValueIterationResourceManager {
  def apply(constants: ResourceManagerConfig, ssc: StreamingContext): ValueIterationResourceManager = {
    new ValueIterationResourceManager(constants, ssc)
  }
}
