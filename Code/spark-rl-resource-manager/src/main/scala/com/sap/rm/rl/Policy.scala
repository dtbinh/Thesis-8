package com.sap.rm.rl

import com.sap.rm.rl.Action.Action

trait Policy {

  def nextActionFrom(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State): Action

}
