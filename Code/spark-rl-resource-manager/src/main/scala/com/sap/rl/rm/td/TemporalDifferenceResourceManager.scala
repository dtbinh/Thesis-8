package com.sap.rl.rm.td

import com.sap.rl.rm.{RLResourceManager, ResourceManagerConfig}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

class TemporalDifferenceResourceManager(val config: ResourceManagerConfig, val streamingContext: StreamingContext) extends RLResourceManager {

  import config._

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    processBatch(batchCompleted.batchInfo)
  }

  def specialize(): Unit = {
    val oldQVal: Double = stateSpace(lastState)(lastAction)
    val currentStateQVal: Double = stateSpace(currentState)(actionToTake)

    val newQVal: Double = ((1 - LearningFactor) * oldQVal) + (LearningFactor * (rewardForLastAction + (DiscountFactor * currentStateQVal)))
    stateSpace.updateQValueForAction(lastState, lastAction, newQVal)

    logQValueUpdate(lastState, lastAction, oldQVal, rewardForLastAction, currentState, actionToTake, currentStateQVal, newQVal)
  }
}

object TemporalDifferenceResourceManager {
  def apply(constants: ResourceManagerConfig, ssc: StreamingContext): TemporalDifferenceResourceManager = {
    new TemporalDifferenceResourceManager(constants, ssc)
  }
}