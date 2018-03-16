package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.listener.BatchListener
import org.apache.spark.streaming.scheduler.listener.SparkListenerT
import org.apache.spark.streaming.scheduler.listener.model.BatchTier

/**
 * Wrapper for original ExecutorAllocationManager making it a subclass of ResourceManager. This
 * class is used only for debugging and performance testing. For regular usage please consult
 * Apache Spark documentation.
 *
 * @param ssc StreamingContext of the application
 */
class SparkResourceManager(ssc: StreamingContext) extends ResourceManager(ssc) {
  override val listener: ExecutorAllocationManager with SparkListenerT =
    new ExecutorAllocationManager(
      executorAllocator,
      ssc.scheduler.receiverTracker,
      sparkConf,
      batchDuration,
      ssc.scheduler.clock
    ) with BatchListener {
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        super[BatchListener].onBatchCompleted(batchCompleted)
        super[ExecutorAllocationManager].onBatchCompleted(batchCompleted)
        log.debug(s"Cluster state: ${getInfo(batches.last)}")
      }
    }

  override def start(): Unit = {
    super.start()
    listener.start()
  }

  override def stop(): Unit = {
    listener.stop()
    super.stop()
  }

  private def getInfo(batch: BatchTier): String = {
    val duration   = batch.duration
    val delay      = batch.delay
    val load       = batch.records
    val executorNo = executors.size

    s"$duration,$delay,$load,$executorNo"
  }
}
