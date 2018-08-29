package com.sap.rm.rl

import com.sap.rm.ResourceManagerLogger
import com.sap.rm.rl.Action.Action

trait Policy extends ResourceManagerLogger {

  def nextActionFrom(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State): Action

}
