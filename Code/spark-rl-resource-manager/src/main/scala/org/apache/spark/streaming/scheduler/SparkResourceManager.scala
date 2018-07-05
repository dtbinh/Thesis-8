package org.apache.spark.streaming.scheduler

import com.sap.rl.rm.{RMConstants, ResourceManager}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.StreamingContext

class SparkResourceManager(val constants: RMConstants, val streamingContext: StreamingContext) extends ResourceManager {

  @transient lazy val log: Logger = LogManager.getLogger(this.getClass)
  private val batchDuration: Long = streamingContext.graph.batchDuration.milliseconds

  val listener: ExecutorAllocationManager = new ExecutorAllocationManager(executorAllocator,
    streamingContext.scheduler.receiverTracker,
    sparkConf,
    batchDuration,
    streamingContext.scheduler.clock)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = listener.onBatchCompleted(batchCompleted)

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
