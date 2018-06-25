package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Reward, State}
import org.apache.spark.streaming.scheduler.RMConstants

class TDReward(constants: RMConstants, stateSpace: TDStateSpace) extends Reward {

  import constants._

  override def forAction(previousState: State, takenAction: Action, currentState: State): Double = {
    val currentLatency: Int = currentState.latency
    val currentExecutors: Int = currentState.numberOfExecutors

    if (currentLatency >= CoarseTargetLatency) {
      (CoarseTargetLatency - currentLatency).toDouble / currentLatency
    } else {
      1.toDouble / currentExecutors
    }
  }
}

object TDReward {
  def apply(constants: RMConstants, stateSpace: TDStateSpace): TDReward = new TDReward(constants, stateSpace)
}
