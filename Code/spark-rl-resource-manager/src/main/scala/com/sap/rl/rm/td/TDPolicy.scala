package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Policy, State}

import scala.collection.mutable

class TDPolicy(stateSpace: TDStateSpace) extends Policy {

  override def nextActionFrom(currentState: State): Action = {
    val qValues: mutable.HashMap[Action, Double] = stateSpace(currentState)

    val maxFunc = { qVal: (Action, Double) => qVal._2 }
    val (bestAction, _) = qValues.maxBy(maxFunc)

    bestAction
  }
}

object TDPolicy {
  def apply(stateSpace: TDStateSpace): TDPolicy = new TDPolicy(stateSpace)
}
