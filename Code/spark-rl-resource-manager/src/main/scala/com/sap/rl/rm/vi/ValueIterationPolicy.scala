package com.sap.rl.rm.vi

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Policy, State}

class ValueIterationPolicy extends Policy {
  override def nextActionFrom(lastState: State, lastAction: Action, currentState: State): Action = ???
}
