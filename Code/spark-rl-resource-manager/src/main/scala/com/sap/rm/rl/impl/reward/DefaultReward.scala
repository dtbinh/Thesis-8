package com.sap.rm.rl.impl.reward

import java.lang.Math.abs

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.Action._
import com.sap.rm.rl.{Reward, State, StateSpace}

class DefaultReward(config: ResourceManagerConfig) extends Reward {

  import config._

  override def forAction(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State): Double = {
    val currentStateDiff = dangerZoneLatencyDifference(currentState)
    val lastStateDiff = dangerZoneLatencyDifference(lastState)

    if (isStateInDangerZone(currentState)) {
      if ((lastAction == ScaleOut && (currentState.loadIsIncreasing ||
                                     (isStateInDangerZone(lastState) && currentStateDiff < lastStateDiff))) ||
          (lastAction == NoAction && !currentState.loadIsIncreasing) ||
          (lastAction == NoAction && lastState.numberOfExecutors == MaximumExecutors && InitializationMode != "zero"))
        return currentStateDiff
      return -currentStateDiff
    }

    if (!isStateInDangerZone(lastState) && !isStateInDangerZone(currentState) && lastAction == ScaleOut) {
      return -MaximumExecutors.toDouble / currentState.numberOfExecutors
    }

    if (lastAction == NoAction) {
      return 1
    }

    MaximumExecutors.toDouble / currentState.numberOfExecutors
  }

  def dangerZoneLatencyDifference(s: State): Double = abs(CoarseTargetLatency - s.latency - 1)

  def isStateInDangerZone(s: State): Boolean = s.latency >= CoarseTargetLatency
}

object DefaultReward {
  def apply(config: ResourceManagerConfig): DefaultReward = new DefaultReward(config)
}
