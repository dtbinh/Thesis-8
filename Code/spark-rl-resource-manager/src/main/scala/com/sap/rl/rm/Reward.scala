package com.sap.rl.rm

import com.sap.rl.rm.Action.Action

trait Reward {

  def forAction(lastState: State, lastAction: Action, currentState: State): Double

}
