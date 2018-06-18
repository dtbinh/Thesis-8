package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.StreamingContext

class SparkResourceManager(ssc: StreamingContext) extends ResourceManager(ssc) {
  override val listener: ExecutorAllocationManager = new ExecutorAllocationManager(
    executorAllocator,
    ssc.scheduler.receiverTracker,
    sparkConf,
    batchDuration,
    ssc.scheduler.clock
  ) {
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      super.onBatchCompleted(batchCompleted)
    }
  }

  override def start(): Unit = {
    super.start()
    listener.start()
  }

  override def stop(): Unit = {
    super.stop()
    listener.stop()
  }
}
