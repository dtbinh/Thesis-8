package com.sap.rm.rl

import com.sap.rm.rl.Action.Action
import com.sap.rm.rl.impl.policy.GreedyPolicy
import com.sap.rm.rl.impl.reward.DefaultReward
import com.sap.rm.{ResourceManager, ResourceManagerConfig}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

import scala.collection.mutable.{HashMap => MutableHashMap, HashSet => MutableHashSet}

class ValueIterationResourceManager(
                                     cfg: ResourceManagerConfig,
                                     ssc: StreamingContext,
                                     stateSpace: StateSpace,
                                     policy: Policy,
                                     reward: Reward
                                   ) extends TemporalDifferenceResourceManager(cfg, ssc, stateSpace, policy, reward) {

  private lazy val stateActionCount = MutableHashMap[(State, Action), Int]().withDefaultValue(0)
  private lazy val stateActionReward = MutableHashMap[(State, Action), Double]().withDefaultValue(0)
  private lazy val stateActionStateCount = MutableHashMap[(State, Action, State), Int]().withDefaultValue(0)
  private lazy val stateActionRewardBar = MutableHashMap[(State, Action), Double]().withDefaultValue(0)
  private lazy val stateActionStateBar = MutableHashMap[(State, Action, State), Double]().withDefaultValue(0)
  private lazy val allLandingStates = MutableHashSet[State]()
  private lazy val VValues = MutableHashMap[State, Double]().withDefaultValue(0)

  private var initialized: Boolean = false

  import config._

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    super.onBatchCompleted(batchCompleted)
  }

  override def updateStateSpace(): Unit = {
    if (initialized) {
      // update q-values similar to Q-Learning strategy
      super.updateStateSpace()
      return
    }

    // initialize table
    if (currentBatch.batchTime.milliseconds - streamingStartTime > ValueIterationInitializationTime) {
      initialized = true

      stateActionReward.foreach { kv =>
        val stateAction: (State, Action) = kv._1
        val reward: Double = kv._2

        stateActionRewardBar += stateAction -> reward / stateActionCount(stateAction)
      }

      stateActionStateCount.foreach { kv =>
        val stateActionState: (State, Action, State) = kv._1
        val count: Double = kv._2.toDouble

        stateActionStateBar += stateActionState -> count / stateActionCount((stateActionState._1, stateActionState._2))
        allLandingStates += stateActionState._3
      }

      0 to ValueIterationInitializationCount foreach { _ =>
        stateActionCount.foreach { kv =>
          val stateAction: (State, Action) = kv._1
          val startingState = kv._1._1
          val action = kv._1._2

          var sum: Double = 0
          for (landingState <- allLandingStates) {
            sum += stateActionStateBar((startingState, action, landingState)) * VValues(startingState)
          }

          val QVal: Double = stateActionRewardBar(stateAction) + DiscountFactor * sum
          stateSpace.updateQValueForAction(startingState, action, QVal)
          VValues(startingState) = stateSpace(startingState).maxBy(_._2)._2
        }
      }

      stateActionCount.clear()
      stateActionReward.clear()
      stateActionStateCount.clear()
      stateActionRewardBar.clear()
      stateActionStateBar.clear()
      allLandingStates.clear()
      VValues.clear()
    } else {
      stateActionCount((lastState, lastAction)) += 1
      stateActionReward((lastState, lastAction)) += rewardForLastAction
      stateActionStateCount((lastState, lastAction, currentState)) += 1
    }
  }
}

object ValueIterationResourceManager {
  def apply(config: ResourceManagerConfig,
            streamingContext: StreamingContext,
            stateSpace: Option[StateSpace] = None,
            policy: Option[Policy] = None,
            reward: Option[Reward] = None
           ): ResourceManager = {
    new ValueIterationResourceManager(
      config,
      streamingContext,
      stateSpace.getOrElse { StateSpaceInitializer.getInstance(config).initialize(StateSpace()) },
      policy.getOrElse(GreedyPolicy(config)),
      reward.getOrElse(DefaultReward(config)))
  }
}
