package com.sap.rl.rm.td

import com.sap.rl.rm.{Action, Reward, State}
import org.apache.spark.streaming.scheduler.RMConstants

class TDReward(constants: RMConstants, stateSpace: TDStateSpace) extends Reward {

  import constants._
  import Action._

  override def forAction(lastState: State, lastAction: Action, currentState: State): Double = {
    if (lastState.latency >= CoarseTargetLatency || currentState.latency >= CoarseTargetLatency) {
      if (lastAction == ScaleOut) BestReward
      else NegativeRewardMultiplier * (CoarseTargetLatency - currentState.latency).toDouble / currentState.latency
    } else {
      if (lastAction == ScaleOut) -BestReward
      else if (currentState.latency < CoarseMinimumLatency && lastAction == Action.ScaleIn) BestReward
      else BestReward / currentState.numberOfExecutors
    }
  }
}

object TDReward {
  def apply(constants: RMConstants, stateSpace: TDStateSpace): TDReward = new TDReward(constants, stateSpace)
}
