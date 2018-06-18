package org.apache.spark.streaming.scheduler

import com.sap.rl.window.Window
import org.apache.spark.internal.Logging

class BatchListener(private val window: Window) extends StreamingListener with Logging {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val totalDelay: Long = batchCompleted.batchInfo.totalDelay.get

    log.info(s"batch finished successfully with delay ${totalDelay}")
    log.debug(batchCompleted.batchInfo.toString)

    window.add(totalDelay)
  }
}
