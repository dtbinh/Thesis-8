package com.sap.rl.rm

import com.sap.rl.rm.Action.Action

trait Reward {

  def forAction(previousState: State, takenAction: Action, currentState: State): Double

}
