package com.sap.rl.rm.impl

import com.sap.rl.rm.Action._
import com.sap.rl.rm.{RMConstants, Reward, State, StateSpace}

class DefaultReward(constants: RMConstants, stateSpace: StateSpace) extends Reward {

  import constants._

  // TODO: fix this shit, consider incoming messages as well
  override def forAction(lastState: State, lastAction: Action, currentState: State): Double = {
    if (isInDangerZone(lastState, currentState)) {
      if (lastAction == ScaleOut)
        BestReward
      // if we are in danger zone, no matter which action to take, it is really bad
      else
        RewardMultiplier * (CoarseTargetLatency - currentState.latency).toDouble / currentState.latency
    } else {
      // if we are in safe zone, ScaleOut is the worst possible action
      if (lastAction == ScaleOut)
        -RewardMultiplier * BestReward
      // if we are in absolute safe zone, ScaleIn has the best reward
      else if (isInAbsoluteSafeZone(lastState, currentState) && lastAction == ScaleIn)
        BestReward
      // if both states are in yellow zone, NoAction is better. Don't mess with cluster
      else if (isBothStatesInYellowZone(lastState, currentState) && lastAction == NoAction)
        BestReward
      // if load is increasing, and we are in yellow zone, NoAction is better than ScaleIn
      else if (isLoadIncreasing(lastState, currentState) && isCurrentStateInYellowZone(currentState) && lastAction == NoAction)

      // if incoming messages are increasing, and we are not in yellow zone, ScaleIn is better
      else if (currentState.incomingMessages > lastState.incomingMessages && lastAction == ScaleIn) -BestReward / currentState.numberOfExecutors
      else BestReward / currentState.numberOfExecutors
    }
  }

  def isInDangerZone(lastState: State, currentState: State): Boolean =
    lastState.latency >= CoarseTargetLatency || currentState.latency >= CoarseTargetLatency

  def isInAbsoluteSafeZone(lastState: State, currentState: State): Boolean =
    lastState.latency < CoarseMinimumLatency && currentState.latency < CoarseMinimumLatency

  def isBothStatesInYellowZone(lastState: State, currentState: State): Boolean = {
    val threshold = CoarseTargetLatency - LatencyYellowZoneSteps
    lastState.latency >= threshold && currentState.latency >= threshold
  }

  def isCurrentStateInYellowZone(currentState: State): Boolean =
    currentState.latency >= CoarseTargetLatency - LatencyYellowZoneSteps

  def isLoadDecreasing(lastState: State, currentState: State): Boolean =
    !isLoadIncreasing(lastState, currentState)

  def isLoadIncreasing(lastState: State, currentState: State): Boolean =
    currentState.incomingMessages > lastState.incomingMessages
}

object DefaultReward {
  def apply(constants: RMConstants, stateSpace: StateSpace): DefaultReward = new DefaultReward(constants, stateSpace)
}
