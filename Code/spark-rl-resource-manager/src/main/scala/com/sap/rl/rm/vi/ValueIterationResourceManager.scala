package com.sap.rl.rm.vi

import com.sap.rl.rm.{RLResourceManager, RMConstants}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

class ValueIterationResourceManager(val constants: RMConstants, val streamingContext: StreamingContext) extends RLResourceManager {

  @transient override lazy val log: Logger = LogManager.getLogger(this.getClass)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    super.onBatchCompleted(batchCompleted)
  }

  def specialize(): Unit = {
    // TODO: implement for VI
  }
}

object ValueIterationResourceManager {
  def apply(constants: RMConstants, ssc: StreamingContext): ValueIterationResourceManager = {
    new ValueIterationResourceManager(constants, ssc)
  }
}
