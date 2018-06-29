package com.sap.rl.rm.td

import com.sap.rl.rm.{Action, Policy, State}
import org.apache.spark.streaming.scheduler.RMConstants

class TDPolicy(constants: RMConstants, stateSpace: TDStateSpace) extends Policy {

  import constants._
  import Action._

  override def nextActionFrom(lastState: State, lastAction: Action, currentState: State): Action = {
    if (currentState.latency < CoarseMinimumLatency) return ScaleIn

    val qValues = stateSpace(currentState)

    // monotonicity property
    if (currentState.latency < lastState.latency && lastAction == ScaleIn) maxBy(qValues.filterKeys(_ != ScaleOut))
    else if (currentState.latency > lastState.latency && lastAction == ScaleOut) maxBy(qValues.filterKeys(_ != ScaleIn))
    else maxBy(qValues)
  }

  private def maxBy(qValues: Iterable[(Action, Double)]): Action = qValues.maxBy { _._2 }._1
}

object TDPolicy {
  def apply(constants: RMConstants, stateSpace: TDStateSpace): TDPolicy = new TDPolicy(constants, stateSpace)
}
