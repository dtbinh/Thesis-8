package com.sap.rm.rl.impl.policy

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.Action._
import com.sap.rm.rl.{Policy, State, StateSpace}

class GreedyPolicy(config: ResourceManagerConfig) extends Policy {

  import config._

  override def nextActionFrom(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State): Action = {
    val currentExecutors = currentState.numberOfExecutors
    var qValues = currentExecutors match {
      case MinimumExecutors => stateSpace(currentState).filterKeys(_ != ScaleIn)
      case MaximumExecutors => stateSpace(currentState).filterKeys(_ != ScaleOut)
      case _ => stateSpace(currentState)
    }

    // monotonicity property
    qValues = if (currentState.latency < lastState.latency && lastAction == ScaleIn)
      qValues.filterKeys(_ != ScaleOut)
    else if (currentState.latency > lastState.latency && lastAction == ScaleOut)
      qValues.filterKeys(_ != ScaleIn)
    else qValues

    qValues.maxBy(_._2)._1
  }
}

object GreedyPolicy {
  def apply(config: ResourceManagerConfig): GreedyPolicy = new GreedyPolicy(config)
}
