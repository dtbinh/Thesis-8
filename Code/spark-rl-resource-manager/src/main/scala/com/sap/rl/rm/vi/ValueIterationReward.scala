package com.sap.rl.rm.vi

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Reward, State}

class ValueIterationReward extends Reward {
  override def forAction(lastState: State, lastAction: Action, currentState: State): Double = ???
}
