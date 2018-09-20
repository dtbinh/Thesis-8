package com.sap.rm.rl.impl.reward

import java.lang.Math.{max, min}

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.Action._
import com.sap.rm.rl.{BatchWaitingList, Reward, State, StateSpace}

class PreferNoActionWhenLoadIsDecreasing(val config: ResourceManagerConfig) extends Reward {

  override def forAction(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State, waitingList: BatchWaitingList, numberOfExecutors: Int): Option[Double] = {
    val partialReward: Option[Double] = super.forAction(stateSpace, lastState, lastAction, currentState, waitingList, numberOfExecutors)
    if (partialReward.nonEmpty) {
      return partialReward
    }

    val safeZoneLatencyDiff = safeZoneLatencyDifference(currentState)
    val ratio = executorRatio(numberOfExecutors)

    // current is not in danger zone, taking scale out action when last state was not in danger zone too is really bad.
    if (!isStateInDangerZone(lastState) && lastAction == ScaleOut) {
      return Some(-max(ratio, safeZoneLatencyDiff))
    }

    // positive reward for everything else including NoAction
    Some(min(ratio, safeZoneLatencyDiff))
  }
}

object PreferNoActionWhenLoadIsDecreasing {
  def apply(config: ResourceManagerConfig): PreferNoActionWhenLoadIsDecreasing = new PreferNoActionWhenLoadIsDecreasing(config)
}
