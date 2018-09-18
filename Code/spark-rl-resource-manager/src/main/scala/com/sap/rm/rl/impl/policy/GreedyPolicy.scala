package com.sap.rm.rl.impl.policy

import com.sap.rm.{ResourceManagerConfig, ResourceManagerLogger}
import com.sap.rm.rl.Action._
import com.sap.rm.rl.{BatchWaitingList, Policy, State, StateSpace}

class GreedyPolicy(config: ResourceManagerConfig) extends Policy {

  @transient private lazy val logger = ResourceManagerLogger(config)
  import config._
  import logger._

  override def nextActionFrom(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State, waitingList: BatchWaitingList): Action = {
    if (waitingList.isGrowing) {
      return ScaleOut
    }
    val currentExecutors = currentState.numberOfExecutors
    val stateActionSet = stateSpace(currentState)

    if (stateActionSet.isVisited) {
      var qValues: Seq[(Action, Double)] = removeUnfeasibleActions(currentExecutors, stateActionSet.qValues.toSeq)

      // monotonicity property
      qValues = if (currentState.latency < lastState.latency && lastAction == ScaleIn)
        qValues.filter(_._1 != ScaleOut)
      else if (currentState.latency > lastState.latency && lastAction == ScaleOut)
        qValues.filter(_._1 != ScaleIn)
      else qValues

      logVisitedState(currentState, qValues)

      return qValues.maxBy(_._2)._1
    }

    // look for neighbor state
    var qValues: Seq[(Action, Double)] = stateActionSet.qValues.toSeq ++
      stateSpace(State(currentState.numberOfExecutors - ExecutorGranularity, currentState.latency, currentState.loadIsIncreasing)).qValues.toSeq ++
      stateSpace(State(currentState.numberOfExecutors + ExecutorGranularity, currentState.latency, currentState.loadIsIncreasing)).qValues.toSeq ++
      stateSpace(State(currentState.numberOfExecutors - ExecutorGranularity, currentState.latency + 1, currentState.loadIsIncreasing)).qValues.toSeq ++
      stateSpace(State(currentState.numberOfExecutors, currentState.latency + 1, currentState.loadIsIncreasing)).qValues.toSeq ++
      stateSpace(State(currentState.numberOfExecutors + ExecutorGranularity, currentState.latency + 1, currentState.loadIsIncreasing)).qValues.toSeq ++
      stateSpace(State(currentState.numberOfExecutors - ExecutorGranularity, currentState.latency - 1, currentState.loadIsIncreasing)).qValues.toSeq ++
      stateSpace(State(currentState.numberOfExecutors, currentState.latency - 1, currentState.loadIsIncreasing)).qValues.toSeq ++
      stateSpace(State(currentState.numberOfExecutors + ExecutorGranularity, currentState.latency - 1, currentState.loadIsIncreasing)).qValues.toSeq

    qValues = removeUnfeasibleActions(currentExecutors, qValues)
    logUnvisitedState(currentState, qValues)
    qValues.maxBy(_._2)._1
  }

  private def removeUnfeasibleActions(currentExecutors: Int, qValues: Seq[(Action, Double)]): Seq[(Action, Double)] = {
    currentExecutors match {
      case MinimumExecutors => qValues.filter(_._1 != ScaleIn)
      case MaximumExecutors => qValues.filter(_._1 != ScaleOut)
      case _ => qValues
    }
  }
}

object GreedyPolicy {
  def apply(config: ResourceManagerConfig): GreedyPolicy = new GreedyPolicy(config)
}