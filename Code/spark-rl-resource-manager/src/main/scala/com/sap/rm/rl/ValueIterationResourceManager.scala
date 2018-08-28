package com.sap.rm.rl

import com.sap.rm.rl.Action.Action
import com.sap.rm.rl.impl.reward.DefaultReward
import com.sap.rm.{ResourceManager, ResourceManagerConfig}
import org.apache.spark.streaming.StreamingContext

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
  private lazy val startingStates = MutableHashSet[State]()
  private lazy val landingStates = MutableHashSet[State]()
  private lazy val VValues = MutableHashMap[State, Double]().withDefaultValue(0)

  private var initialized: Boolean = false

  import config._

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
        val (stateAction: (State, Action), reward: Double) = kv
        stateActionRewardBar += stateAction -> reward / stateActionCount(stateAction)
      }

      stateActionStateCount.foreach { kv =>
        val (stateActionState: (State, Action, State), count: Int) = kv
        val stateAction = (stateActionState._1, stateActionState._2)
        val startingState = stateActionState._1
        val landingState = stateActionState._3

        stateActionStateBar += stateActionState -> count.toDouble / stateActionCount(stateAction)
        startingStates += startingState
        landingStates += landingState
      }

      logEverything(
        stateActionCount = stateActionCount,
        stateActionReward = stateActionReward,
        stateActionStateCount = stateActionStateCount,
        stateActionRewardBar = stateActionRewardBar,
        stateActionStateBar = stateActionStateBar,
        startingStates = startingStates,
        landingStates = landingStates)

      0 to ValueIterationInitializationCount foreach { _ =>
        stateActionCount.foreach { kv =>
          val (stateAction: (State, Action), _) = kv
          val (startingState, action) = stateAction

          var sum: Double = 0
          for (landingState <- landingStates) {
            sum += stateActionStateBar((startingState, action, landingState)) * VValues(startingState)
          }

          val QVal: Double = stateActionRewardBar(stateAction) + DiscountFactor * sum
          stateSpace.updateQValueForAction(startingState, action, QVal)
        }

        startingStates.foreach { s => VValues(s) = stateSpace(s).maxBy(_._2)._2 }
        logVValues(VValues)
      }

      stateActionCount.clear()
      stateActionReward.clear()
      stateActionStateCount.clear()
      stateActionRewardBar.clear()
      stateActionStateBar.clear()
      startingStates.clear()
      landingStates.clear()
      VValues.clear()
    } else {
      stateActionCount((lastState, lastAction)) += 1
      stateActionReward((lastState, lastAction)) += rewardForLastAction
      stateActionStateCount((lastState, lastAction, currentState)) += 1

      logStateActionState(
        lastState = lastState,
        lastAction = lastAction,
        rewardForLastAction = rewardForLastAction,
        currentState = currentState,
        stateActionCount = stateActionCount((lastState, lastAction)),
        stateActionReward = stateActionReward((lastState, lastAction)),
        stateActionStateCount = stateActionStateCount((lastState, lastAction, currentState))
      )
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
      stateSpace.getOrElse(StateSpaceInitializer.getInstance(config).initialize(StateSpace())),
      policy.getOrElse(PolicyFactory.getPolicy(config)),
      reward.getOrElse(DefaultReward(config)))
  }
}
