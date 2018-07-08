package com.sap.rl.rm.impl

import com.sap.rl.rm.Action._
import com.sap.rl.rm.{RMConstants, Reward, State, StateSpace}
import org.apache.log4j.LogManager

class DefaultReward(constants: RMConstants, stateSpace: StateSpace) extends Reward {

  import constants._

  @transient private lazy val log = LogManager.getLogger(this.getClass)

  override def forAction(lastState: State, lastAction: Action, currentState: State): Double = {
    if (isInDangerZone(lastState, currentState)) {
      if (lastAction == ScaleOut) {
        log.info("")
        BestReward
      }
      else
        // if we are in danger zone, no matter which action to take, it is still bad.
        RewardMultiplier * (CoarseTargetLatency - currentState.latency).toDouble / currentState.latency
    } else {
      // if we are in absolute safe zone, ScaleIn has the best reward
      if (isInAbsoluteSafeZone(lastState, currentState) && lastAction == ScaleIn) BestReward
      // prefer scale-out when we are in danger zone
      else if (isStateInYellowZone(currentState) && isLoadIncreasing(lastState, currentState))
        if (lastAction == ScaleOut) BestReward
        else -BestReward
      else if (lastAction == ScaleOut) -BestReward
      else if (isLoadDecreasing(lastState, currentState) && lastAction == NoAction) -BestReward
      else BestReward / currentState.numberOfExecutors
    }
  }

  def isInDangerZone(lastState: State, currentState: State): Boolean =
    lastState.latency >= CoarseTargetLatency || currentState.latency >= CoarseTargetLatency

  def isInAbsoluteSafeZone(lastState: State, currentState: State): Boolean =
    lastState.latency < CoarseMinimumLatency && currentState.latency < CoarseMinimumLatency

  def isBothStatesInYellowZone(lastState: State, currentState: State): Boolean =
    isStateInYellowZone(lastState) && isStateInYellowZone(currentState)

  def isStateInYellowZone(s: State): Boolean = s.latency >= CoarseTargetLatency - LatencyYellowZoneSteps

  def isLoadIncreasing(lastState: State, currentState: State): Boolean =
    currentState.incomingMessages > lastState.incomingMessages

  def isLoadDecreasing(lastState: State, currentState: State): Boolean =
    currentState.incomingMessages < lastState.incomingMessages
}

object DefaultReward {
  def apply(constants: RMConstants, stateSpace: StateSpace): DefaultReward = new DefaultReward(constants, stateSpace)
}
