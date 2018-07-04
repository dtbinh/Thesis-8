package com.sap.rl.rm.td

import com.sap.rl.rm.ResourceManager
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._

class TemporalDifferenceResourceManager(val constants: RMConstants, val streamingContext: StreamingContext) extends ResourceManager {

  import constants._

  @transient lazy val log: Logger = LogManager.getLogger(this.getClass)

  override def specialize(): Unit = {
    super.specialize()

    val oldQVal: Double = stateSpace(lastState)(lastAction)
    val currentStateQVal: Double = stateSpace(currentState)(actionToTake)

    val newQVal: Double = ((1 - LearningFactor) * oldQVal) + (LearningFactor * (rewardForLastAction + (DiscountFactor * currentStateQVal)))
    stateSpace.updateQValueForAction(lastState, lastAction, newQVal)

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
  def apply(ssc: StreamingContext): TemporalDifferenceResourceManager = {
    val constants: RMConstants = RMConstants(ssc.sparkContext.getConf)
    new TemporalDifferenceResourceManager(constants, ssc)
  }
}