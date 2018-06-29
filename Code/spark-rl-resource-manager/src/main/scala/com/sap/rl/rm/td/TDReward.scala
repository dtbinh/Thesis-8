package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Action, Reward, State}
import org.apache.spark.streaming.scheduler.RMConstants

class TDReward(constants: RMConstants, stateSpace: TDStateSpace) extends Reward {

  import constants._

  override def forAction(lastState: State, lastAction: Action, currentState: State): Double = {
    var reward: Double = 0

    if (lastState.latency > CoarseTargetLatency || currentState.latency > CoarseTargetLatency) {
      if (lastAction == Action.ScaleOut) {
        reward = BestReward
      } else {
        reward = (CoarseTargetLatency - currentState.latency).toDouble / currentState.latency
      }
    } else if (lastState.latency < CoarseTargetLatency && currentState.latency < CoarseTargetLatency) {
      if (lastAction == Action.ScaleOut) {
        reward = -BestReward
      } else if (currentState.latency < CoarseMinimumLatency && lastAction == Action.ScaleIn) {
        reward = BestReward
      } else {
        reward = BestReward / currentState.numberOfExecutors
      }
    }

    reward
  }
}

object TDReward {
  def apply(constants: RMConstants, stateSpace: TDStateSpace): TDReward = new TDReward(constants, stateSpace)
}
