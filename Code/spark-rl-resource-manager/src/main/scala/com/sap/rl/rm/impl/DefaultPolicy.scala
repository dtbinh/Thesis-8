package com.sap.rl.rm.impl

import com.sap.rl.rm.Action._
import com.sap.rl.rm.{Policy, ResourceManagerConfig, State, StateSpace}

class DefaultPolicy(config: ResourceManagerConfig, stateSpace: StateSpace) extends Policy {

  import config._

  override def nextActionFrom(lastState: State, lastAction: Action, currentState: State): Action = {
    val currentExecutors = currentState.numberOfExecutors
    val qValues = currentExecutors match {
      case MinimumExecutors =>
        stateSpace(currentState).filterKeys {
          _ != ScaleIn
        }
      case MaximumExecutors =>
        stateSpace(currentState).filterKeys {
          _ != ScaleOut
        }
      case _ => stateSpace(currentState)
    }

    // monotonicity property
    if (currentState.latency < lastState.latency && lastAction == ScaleIn)
      qValues.filterKeys {
        _ != ScaleOut
      }.maxBy {
        _._2
      }._1
    else if (currentState.latency > lastState.latency && lastAction == ScaleOut)
      qValues.filterKeys {
        _ != ScaleIn
      }.maxBy {
        _._2
      }._1
    else
      qValues.maxBy {
        _._2
      }._1
  }
}

object DefaultPolicy {
  def apply(constants: ResourceManagerConfig, stateSpace: StateSpace): DefaultPolicy = new DefaultPolicy(constants, stateSpace)
}
