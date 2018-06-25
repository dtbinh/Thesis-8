package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Policy, State}

class TDPolicy(stateSpace: TDStateSpace) extends Policy {

  override def nextActionFrom(currentState: State): Action = {
    val qValues = stateSpace.value(currentState)

    val bestQValue: QValue = qValues.maxBy(qValue => qValue.expectedReward)

    bestQValue.action
  }
}

object TDPolicy {
  def apply(stateSpace: TDStateSpace): TDPolicy = new TDPolicy(stateSpace)
}
