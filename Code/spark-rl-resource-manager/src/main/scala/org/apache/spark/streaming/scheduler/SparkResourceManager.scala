package org.apache.spark.streaming.scheduler

import com.sap.rl.rm.{RMConstants, ResourceManager}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart}
import org.apache.spark.streaming.StreamingContext

class SparkResourceManager(val constants: RMConstants, val streamingContext: StreamingContext) extends ResourceManager {

  @transient override lazy val log: Logger = LogManager.getLogger(this.getClass)
  private val batchDuration: Long = streamingContext.graph.batchDuration.milliseconds

  val listener: ExecutorAllocationManager = new ExecutorAllocationManager(executorAllocator,
    streamingContext.scheduler.receiverTracker,
    sparkConf,
    batchDuration,
    streamingContext.scheduler.clock)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = listener.onBatchCompleted(batchCompleted)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    super[ResourceManager].onApplicationStart(applicationStart)
    listener.start()
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    super[ResourceManager].onApplicationEnd(applicationEnd)
    listener.stop()
  }
}

object SparkResourceManager {
  def apply(constants: RMConstants, ssc: StreamingContext): SparkResourceManager = {
    new SparkResourceManager(constants, ssc)
  }
}
