package com.sap.rm.rl.impl.reward

import java.lang.Math.{min, max}

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.Action._
import com.sap.rm.rl.{Reward, State, StateSpace}

class PreferNoActionWhenLoadIsDecreasing(val config: ResourceManagerConfig) extends Reward {

  override def forAction(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State): Option[Double] = {
    val partialReward: Option[Double] = super.forAction(stateSpace, lastState, lastAction, currentState)
    if (partialReward.nonEmpty) {
      return partialReward
    }

    val safeZoneLatencyDiff = safeZoneLatencyDifference(currentState)
    val ratio = executorRatio(currentState)

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
