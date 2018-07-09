package com.sap.rl.rm.impl

import java.lang.Math.abs

import com.sap.rl.rm.Action._
import com.sap.rl.rm.RMConstants._
import com.sap.rl.rm.{RMConstants, Reward, State, StateSpace}

class DefaultReward(constants: RMConstants, stateSpace: StateSpace) extends Reward {

  import constants._

  override def forAction(lastState: State, lastAction: Action, currentState: State): Double = {
    if (isStateInDangerZone(currentState))
      if (lastAction == ScaleOut)
        abs(dangerZoneLatencyDifference(currentState))
      else
        dangerZoneLatencyDifference(currentState)
    else
      safeZoneLatencyDifference(currentState) * LatencyGranularity / currentState.numberOfExecutors
  }

  def incomingMessageDifference(lastState: State, currentState: State): Int = currentState.incomingMessages - lastState.incomingMessages

  def dangerZoneLatencyDifference(s: State): Double = CoarseTargetLatency - (s.latency + One)

  def safeZoneLatencyDifference(s: State): Double = CoarseTargetLatency - s.latency

  def isStateInDangerZone(s: State): Boolean = s.latency >= CoarseTargetLatency
}

object DefaultReward {
  def apply(constants: RMConstants, stateSpace: StateSpace): DefaultReward = new DefaultReward(constants, stateSpace)
}
