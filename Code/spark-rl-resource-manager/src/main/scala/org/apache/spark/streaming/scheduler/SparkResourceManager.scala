package org.apache.spark.streaming.scheduler

import com.sap.rl.rm.{ResourceManagerConfig, ResourceManager}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart}
import org.apache.spark.streaming.StreamingContext

class SparkResourceManager(val config: ResourceManagerConfig, val streamingContext: StreamingContext) extends ResourceManager {

  private lazy val batchDuration: Long = streamingContext.graph.batchDuration.milliseconds

  lazy val listener: ExecutorAllocationManager = new ExecutorAllocationManager(client,
    streamingContext.scheduler.receiverTracker,
    sparkConf,
    batchDuration,
    streamingContext.scheduler.clock)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    super.onApplicationStart(applicationStart)
    listener.start()
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(applicationEnd)
    listener.stop()
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    if (processBatch(batchCompleted.batchInfo)) {
      listener.onBatchCompleted(batchCompleted)
    }
  }
}

object SparkResourceManager {
  def apply(constants: ResourceManagerConfig, ssc: StreamingContext): SparkResourceManager = {
    new SparkResourceManager(constants, ssc)
  }
}
