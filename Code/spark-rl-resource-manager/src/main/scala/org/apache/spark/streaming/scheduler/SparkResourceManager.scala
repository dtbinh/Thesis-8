package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.StreamingContext

class SparkResourceManager(constants: RMConstants, ssc: StreamingContext) extends ResourceManager(constants, ssc) {
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

object SparkResourceManager {
  def apply(ssc: StreamingContext): SparkResourceManager = {
    val constants: RMConstants = RMConstants(ssc.conf)
    new SparkResourceManager(constants, ssc)
  }
}
