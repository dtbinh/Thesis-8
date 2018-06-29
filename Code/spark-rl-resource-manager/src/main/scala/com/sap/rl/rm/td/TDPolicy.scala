package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Action, Policy, State}
import org.apache.spark.streaming.scheduler.RMConstants

import scala.collection.mutable

class TDPolicy(constants: RMConstants, stateSpace: TDStateSpace) extends Policy {

  import constants._

  override def nextActionFrom(lastState: State, lastAction: Action, currentState: State): Action = {
    var bestAction: Option[Action] = None

    if (currentState.latency < CoarseMinimumLatency) {
      bestAction = Some(Action.ScaleIn)
    } else {
      val qValues: mutable.HashMap[Action, Double] = stateSpace(currentState)

      // monotonicity property
      if (currentState.latency < lastState.latency && lastAction == Action.ScaleIn) {
        bestAction = Some(qValues.filterKeys(_ != Action.ScaleOut).maxBy { _._2 }._1)
      } else if (currentState.latency > lastState.latency && lastAction == Action.ScaleOut) {
        bestAction = Some(qValues.filterKeys(_ != Action.ScaleIn).maxBy { _._2 }._1)
      }

      if (bestAction.isEmpty) bestAction = Some(qValues.maxBy { qVal: (Action, Double) => qVal._2 }._1)
    }

    bestAction.get
  }
}

object TDPolicy {
  def apply(constants: RMConstants, stateSpace: TDStateSpace): TDPolicy = new TDPolicy(constants, stateSpace)
}
