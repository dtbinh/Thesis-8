package com.sap.rl.rm.impl

import com.sap.rl.rm.Action._
import com.sap.rl.rm.{RMConstants, Reward, State, StateSpace}

class DefaultReward(constants: RMConstants, stateSpace: StateSpace) extends Reward {

  import constants._

  override def forAction(lastState: State, lastAction: Action, currentState: State): Double = {
    if (lastState.latency >= CoarseTargetLatency || currentState.latency >= CoarseTargetLatency) {
      if (lastAction == ScaleOut) BestReward
      else NegativeRewardMultiplier * (CoarseTargetLatency - currentState.latency).toDouble / currentState.latency
    } else {
      if (lastAction == ScaleOut) -BestReward
      else if (currentState.latency < CoarseMinimumLatency && lastAction == ScaleIn) BestReward
      else BestReward / currentState.numberOfExecutors
    }
  }
}

object DefaultReward {
  def apply(constants: RMConstants, stateSpace: StateSpace): DefaultReward = new DefaultReward(constants, stateSpace)
}
