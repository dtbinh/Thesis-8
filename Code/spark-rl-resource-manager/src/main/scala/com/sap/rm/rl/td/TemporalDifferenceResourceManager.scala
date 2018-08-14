package com.sap.rm.rl.td

import com.sap.rm.{ResourceManager, ResourceManagerConfig}
import com.sap.rm.rl.impl.{DefaultPolicy, DefaultReward}
import com.sap.rm.rl._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

class TemporalDifferenceResourceManager(
                                         val config: ResourceManagerConfig,
                                         val streamingContext: StreamingContext,
                                         val stateSpace: StateSpace,
                                         val policy: Policy,
                                         val reward: Reward
                                       ) extends RLResourceManager {

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
  def apply(config: ResourceManagerConfig,
            streamingContext: StreamingContext,
            stateSpace: Option[StateSpace] = None,
            policy: Option[Policy] = None,
            reward: Option[Reward] = None
           ): ResourceManager = {

    new TemporalDifferenceResourceManager(
      config,
      streamingContext,
      stateSpace.getOrElse { StateSpaceInitializer.getInstance(config).initialize(StateSpace()) },
      policy.getOrElse(DefaultPolicy(config)),
      reward.getOrElse(DefaultReward(config)))
  }
}