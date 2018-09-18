package com.sap.rm.rl

import com.sap.rm.rl.Action.Action
import com.sap.rm.rl.Action._
import com.sap.rm.rl.impl.statespace.StateSpaceUtils

trait Reward extends StateSpaceUtils {

  import config._

  def forAction(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State, waitingList: BatchWaitingList): Option[Double] = {
    if (isStateInDangerZone(currentState)) {
      val currentStateDiff = dangerZoneLatencyDifference(currentState)
      val lastStateDiff = dangerZoneLatencyDifference(lastState)

      if ((lastAction == ScaleOut && (currentState.loadIsIncreasing || waitingList.isGrowing  || waitingList.isShrinking || (isStateInDangerZone(lastState) && currentStateDiff < lastStateDiff))) ||
          (lastAction == NoAction && (currentState.latency < lastState.latency || lastState.numberOfExecutors == MaximumExecutors))) {
        return Some(currentStateDiff)
      }
      return Some(-currentStateDiff)
    }

    None
  }
}
