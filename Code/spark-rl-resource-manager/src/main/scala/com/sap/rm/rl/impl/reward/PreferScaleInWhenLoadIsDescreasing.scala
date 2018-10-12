package com.sap.rm.rl.impl.reward

import java.lang.Math.max

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.Action.Action
import com.sap.rm.rl.Action._
import com.sap.rm.rl.{BatchWaitingList, Reward, State, StateSpace}

class PreferScaleInWhenLoadIsDescreasing(val config: ResourceManagerConfig) extends Reward {

  import config._

  override def forAction(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State, waitingList: BatchWaitingList, numberOfExecutors: Int): Option[Double] = {
    val partialReward: Option[Double] = super.forAction(stateSpace, lastState, lastAction, currentState, waitingList, numberOfExecutors)
    if (partialReward.nonEmpty) {
      return partialReward
    }

    val safeZoneLatencyDiff = safeZoneLatencyDifference(currentState)
    val ratio = executorRatio(numberOfExecutors)

    // current is not in danger zone, now check for last state
    if (!isStateInDangerZone(lastState) && lastAction == NoAction && !currentState.loadIsIncreasing && numberOfExecutors != MinimumExecutors) {
      // if latency is decreasing, then no action gets negative reward
      if ((currentState.latency < lastState.latency) || (currentState.latency == lastState.latency && currentState.latency == 0)) {
        return Some(-max(ratio, safeZoneLatencyDiff))
      }
    }

    // if scale out didn't improve latency, then negative reward
    if (lastAction == ScaleOut && currentState.latency >= lastState.latency && !currentState.loadIsIncreasing) {
      return Some(-max(ratio, safeZoneLatencyDiff))
    }

    // positive reward for everything else
    if (lastAction == NoAction && numberOfExecutors == MinimumExecutors) {
      return Some(NoReward)
    }
    Some(1)
  }
}

object PreferScaleInWhenLoadIsDescreasing {
  def apply(config: ResourceManagerConfig): PreferScaleInWhenLoadIsDescreasing = new PreferScaleInWhenLoadIsDescreasing(config)
}