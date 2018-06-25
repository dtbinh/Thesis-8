package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Policy, State}

class TDPolicy(stateSpace: TDStateSpace) extends Policy {

  override def nextActionFrom(currentState: State): Action = ???

}
