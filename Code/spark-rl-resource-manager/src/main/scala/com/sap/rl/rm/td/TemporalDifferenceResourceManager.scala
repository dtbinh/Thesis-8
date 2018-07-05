package com.sap.rl.rm.td

import com.sap.rl.rm.{RMConstants, ResourceManager}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.StreamingContext

class TemporalDifferenceResourceManager(val constants: RMConstants, val streamingContext: StreamingContext) extends ResourceManager {

  import constants._

  @transient override lazy val log: Logger = LogManager.getLogger(this.getClass)

  override def specialize(): Unit = {
    super.specialize()

    val oldQVal: Double = stateSpace(lastState.get)(lastAction.get)
    val currentStateQVal: Double = stateSpace(currentState.get)(actionToTake.get)

    val newQVal: Double = ((1 - LearningFactor) * oldQVal) + (LearningFactor * (rewardForLastAction.get + (DiscountFactor * currentStateQVal)))
    stateSpace.updateQValueForAction(lastState.get, lastAction.get, newQVal)

    log.info(
      s""" --- QValue-Update-Begin ---
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