package com.sap.rm.rl

import com.sap.rm.rl.Action.Action
import com.sap.rm.rl.Action._
import com.sap.rm.rl.impl.statespace.StateSpaceUtils

trait Reward extends StateSpaceUtils {

  import config._

  def forAction(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State, waitingList: BatchWaitingList, numberOfExecutors: Int): Option[Double] = {
    if (isStateInDangerZone(currentState)) {
      val currentStateDiff = dangerZoneLatencyDifference(currentState)

      if (currentState.latency < lastState.latency || waitingList.isShrinking) {
        return Some(currentStateDiff)
      }

      if (lastAction == ScaleOut && (currentState.latency == lastState.latency || currentState.loadIsIncreasing)) {
        return Some(currentStateDiff)
      }

      if (lastAction == NoAction) {
        if (numberOfExecutors == MaximumExecutors) {
          return Some(BestReward)
        }
        if (currentState.latency == lastState.latency && !currentState.loadIsIncreasing) {
          return Some(currentStateDiff)
        }
      }

      return Some(-currentStateDiff)
    }

    None
  }
}
