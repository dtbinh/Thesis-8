package com.sap.rl.rm

import com.sap.rl.rm.Action.Action

trait Policy {

  def nextActionFrom(currentState: State): Action

}
