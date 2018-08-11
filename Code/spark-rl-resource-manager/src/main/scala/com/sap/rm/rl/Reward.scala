package com.sap.rm.rl

import com.sap.rm.rl.Action.Action

trait Reward {

  def forAction(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State): Double

}
