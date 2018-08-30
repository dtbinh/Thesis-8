package com.sap.rm.rl.impl.reward

import java.lang.Math.{max, min}

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.Action.Action
import com.sap.rm.rl.Action._
import com.sap.rm.rl.{Reward, State, StateSpace}

class PreferScaleInWhenLoadIsDescreasing(val config: ResourceManagerConfig) extends Reward {

  override def forAction(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State): Option[Double] = {
    val partialReward: Option[Double] = super.forAction(stateSpace, lastState, lastAction, currentState)
    if (partialReward.nonEmpty) {
      return partialReward
    }

    val safeZoneLatencyDiff = safeZoneLatencyDifference(currentState)
    val ratio = executorRatio(currentState)

    // current is not in danger zone, now check for last state
    if (!isStateInDangerZone(lastState)) {
      // taking scale out action when last state was not in danger zone too is really bad.
      if (lastAction == ScaleOut) {
        return Some(-max(ratio, safeZoneLatencyDiff))
      }

      // if load or latency is decreasing, then no action gets negative reward
      if ((currentState.latency < lastState.latency || !currentState.loadIsIncreasing) && lastAction == NoAction) {
        return Some(-ratio)
      }
    }

    // positive reward for everything else
    Some(min(ratio, safeZoneLatencyDiff))
  }
}

object PreferScaleInWhenLoadIsDescreasing {
  def apply(config: ResourceManagerConfig): PreferScaleInWhenLoadIsDescreasing = new PreferScaleInWhenLoadIsDescreasing(config)
}