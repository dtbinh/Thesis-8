package com.sap.rm.rl

import Action._

import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}

@SerialVersionUID(1L)
class StateSpace(value: MutableHashMap[State, MutableHashMap[Action, Double]]) extends Serializable {

  def updateQValueForAction(state: State, action: Action, qVal: Double): Unit = {
    val qValues = value(state)
    qValues(action) = qVal
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
        mutable.HashMap(
          ScaleOut -> scaleOutReward,
          NoAction -> noActionReward,
          ScaleIn -> scaleInReward
        )
      )
  }

  def size: Int = value.size

  def apply(s: State): MutableHashMap[Action, Double] = value(s)

  def apply(s: State, a: Action): Double = value(s)(a)

  override def toString: String = {
    val content = StringBuilder.newBuilder
    val sep = System.getProperty("line.separator")

    for ((k: State, v: MutableHashMap[Action, Double]) <- value.toList.sortWith(_._1 < _._1)) {
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
  def apply(): StateSpace = new StateSpace(MutableHashMap[State, MutableHashMap[Action, Double]]())
}