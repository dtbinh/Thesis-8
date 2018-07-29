package com.sap.rl.rm.td

import com.sap.rl.rm.LogStatus._
import com.sap.rl.rm.{RLResourceManager, RMConstants}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

class TemporalDifferenceResourceManager(val constants: RMConstants, val streamingContext: StreamingContext) extends RLResourceManager {

  import constants._

  @transient override lazy val log: Logger = LogManager.getLogger(this.getClass)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    processBatch(batchCompleted.batchInfo)
  }

  def specialize(): Unit = {
    val oldQVal: Double = stateSpace(lastState)(lastAction)
    val currentStateQVal: Double = stateSpace(currentState)(actionToTake)

    val newQVal: Double = ((1 - LearningFactor) * oldQVal) + (LearningFactor * (rewardForLastAction + (DiscountFactor * currentStateQVal)))
    stateSpace.updateQValueForAction(lastState, lastAction, newQVal)

    log.info(
      s""" --- $MY_TAG -- QValue-Update-Begin ---
         | ==========================
         | lastState=$lastState
         | lastAction=$lastAction
         | oldQValue=$oldQVal
         | reward=$rewardForLastAction
         | ==========================
         | currentState=$currentState
         | actionTotake=$actionToTake
         | currentStateQValue=$currentStateQVal
         | ==========================
         | newQValue=$newQVal
         | ==========================
         | --- QValue-Update-End ---""".stripMargin)
  }
}

object TemporalDifferenceResourceManager {
  def apply(constants: RMConstants, ssc: StreamingContext): TemporalDifferenceResourceManager = {
    new TemporalDifferenceResourceManager(constants, ssc)
  }
}