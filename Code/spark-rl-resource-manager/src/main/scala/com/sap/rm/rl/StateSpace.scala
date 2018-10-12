package com.sap.rm.rl

import Action._
import com.sap.rm.ResourceManagerConfig.{Equal, GreaterThan, LessThan}

import scala.collection.mutable.{HashMap => MutableHashMap}

@SerialVersionUID(9586967L)
case class State(latency: Int, loadIsIncreasing: Boolean) extends Serializable with Ordered[State] {

  override def compare(that: State): Int = {
    if (latency < that.latency) LessThan
    else if (latency > that.latency) GreaterThan
    else if (!loadIsIncreasing) LessThan
    else if (loadIsIncreasing) GreaterThan
    else Equal
  }

  override def toString: String = "State(Latency=%d,loadIsIncreasing=%b)".format(latency, loadIsIncreasing)
}

@SerialVersionUID(3123539L)
case class StateActionSet(var isVisited: Boolean = false, qValues: MutableHashMap[Action, Double]) extends Serializable {
  override def toString: String = "isVisited(%b) - qValues(%s)".format(isVisited, qValues.toString())
}

@SerialVersionUID(4933078L)
class StateSpace(value: MutableHashMap[State, StateActionSet]) extends Serializable {

  def updateQValueForAction(state: State, action: Action, qVal: Double): Unit = {
    val stateActionSet = value(state)
    stateActionSet.isVisited = true
    stateActionSet.qValues(action) = qVal
  }

  def addState(
                latency: Int,
                loadIsIncreasing: Boolean,
                scaleOutReward: Double,
                noActionReward: Double,
                scaleInReward: Double
              ): Unit = {

    value += (
      State(latency, loadIsIncreasing) ->
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