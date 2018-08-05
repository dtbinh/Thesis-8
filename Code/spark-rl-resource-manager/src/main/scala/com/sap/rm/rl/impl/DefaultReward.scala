package com.sap.rm.rl.impl

import java.lang.Math.abs

import com.sap.rm.rl.Action._
import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{Reward, State, StateSpace}

class DefaultReward(config: ResourceManagerConfig, stateSpace: StateSpace) extends Reward {

  import config._

  override def forAction(lastState: State, lastAction: Action, currentState: State): Double = {
    if (isStateInDangerZone(currentState))
      if (lastAction == ScaleOut)
        abs(dangerZoneLatencyDifference(currentState))
      else
        dangerZoneLatencyDifference(currentState)
    else
      (MaximumExecutors.toDouble / currentState.numberOfExecutors) - 1
  }

  def dangerZoneLatencyDifference(s: State): Double = CoarseTargetLatency - s.latency - 1

  def isStateInDangerZone(s: State): Boolean = s.latency >= CoarseTargetLatency
}

object DefaultReward {
  def apply(constants: ResourceManagerConfig, stateSpace: StateSpace): DefaultReward = new DefaultReward(constants, stateSpace)
}
