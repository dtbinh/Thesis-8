package com.sap.rm.rl

import Action._
import scala.collection.mutable.{HashMap => MutableHashMap}

case class StateActionSet(var isVisited: Boolean = false, qValues: MutableHashMap[Action, Double]) {
  override def toString: String = "isVisited(%b) - qValues(%s)".format(isVisited, qValues.toString())
}

@SerialVersionUID(1L)
class StateSpace(value: MutableHashMap[State, StateActionSet]) extends Serializable {

  def updateQValueForAction(state: State, action: Action, qVal: Double): Unit = {
    val stateActionSet = value(state)
    stateActionSet.isVisited = true
    stateActionSet.qValues(action) = qVal
  }

  def addState(
                numberOfExecutors: Int,
                latency: Int,
                loadIsIncreasing: Boolean,
                scaleOutReward: Double,
                noActionReward: Double,
                scaleInReward: Double
              ): Unit = {

    value += (
      State(numberOfExecutors, latency, loadIsIncreasing) ->
        StateActionSet(
          qValues = MutableHashMap(ScaleOut -> scaleOutReward, NoAction -> noActionReward, ScaleIn -> scaleInReward)
        )
      )
  }

  def setVisited(state: State): Unit = value(state).isVisited = true

  def size: Int = value.size

  def apply(s: State): StateActionSet = {
    if (!value.contains(s)) {
      // return empty action set
      StateActionSet(isVisited = false, MutableHashMap[Action, Double]())
    } else {
      value(s)
    }
  }

  def apply(s: State, a: Action): Double = value(s).qValues(a)

  override def toString: String = {
    val content = StringBuilder.newBuilder
    val sep = System.getProperty("line.separator")

    for ((k: State, v: StateActionSet) <- value.toList.sortWith(_._1 < _._1)) {
      content.append(k)
      content.append(" => ")
      content.append(v)
      content.append(sep + " ")
    }

    content.delete(content.lastIndexOf(sep), content.length)

    s"""TDStateSpace
       | size: $size
       | ----------------
       | ${content.toString()}
       | ----------------
     """.stripMargin
  }
}

object StateSpace {
  def apply(): StateSpace = new StateSpace(MutableHashMap[State, StateActionSet]())
}