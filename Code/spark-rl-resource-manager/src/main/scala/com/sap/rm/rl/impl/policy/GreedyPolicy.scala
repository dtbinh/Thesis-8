package com.sap.rm.rl.impl.policy

import com.sap.rm.{ResourceManagerConfig, ResourceManagerLogger}
import com.sap.rm.rl.Action._
import com.sap.rm.rl.{BatchWaitingList, Policy, State, StateSpace}

class GreedyPolicy(config: ResourceManagerConfig) extends Policy {

  @transient private lazy val logger = ResourceManagerLogger(config)
  import config._
  import logger._

  override def nextActionFrom(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State, waitingList: BatchWaitingList, numberOfExecutors: Int): Action = {
    val stateActionSet = stateSpace(currentState)
    var qValues: Seq[(Action, Double)] = removeUnfeasibleActions(numberOfExecutors, currentState, stateActionSet.qValues.toSeq)

    // monotonicity property
    qValues = if (currentState.latency < lastState.latency && lastAction == ScaleIn)
      qValues.filter(_._1 != ScaleOut)
    else if (currentState.latency > lastState.latency && lastAction == ScaleOut)
      qValues.filter(_._1 != ScaleIn)
    else qValues

    if (stateActionSet.isVisited) {
      logVisitedState(currentState, qValues)
    } else {
      logUnvisitedState(currentState, qValues)
    }

    qValues.maxBy(_._2)._1
  }

  private def removeUnfeasibleActions(numberOfExecutors: Int, currentState: State, qValues: Seq[(Action, Double)]): Seq[(Action, Double)] = {
    numberOfExecutors match {
      case MinimumExecutors => {
        logExecutorNotEnough(numberOfExecutors, currentState)
        qValues.filter(_._1 != ScaleIn)
      }
      case MaximumExecutors => {
        logNoMoreExecutorsLeft(numberOfExecutors, currentState)
        qValues.filter(_._1 != ScaleOut)
      }
      case _ => qValues
    }
  }
}

object GreedyPolicy {
  def apply(config: ResourceManagerConfig): GreedyPolicy = new GreedyPolicy(config)
}