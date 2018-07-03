package com.sap.rl.rm.td

import com.sap.rl.rm.Action._
import com.sap.rl.rm.{Action, State}
import org.apache.spark.streaming.scheduler.RMConstants

import scala.collection.mutable.{HashMap => MutableHashMap}

import scala.collection.mutable

class TemporalDifferenceStateSpace(value: MutableHashMap[State, MutableHashMap[Action, Double]]) {

  def updateQValueForAction(state: State, action: Action, qVal: Double): Unit = {
    val qValues = value(state)
    qValues(action) = qVal
  }

  def apply(s: State): MutableHashMap[Action, Double] = value(s)

  def apply(s: State, a: Action): Double = value(s)(a)

  def size: Int = value.size

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

object TemporalDifferenceStateSpace {
  def apply(constants: RMConstants): TemporalDifferenceStateSpace = {
    import constants._

    val space: MutableHashMap[State, MutableHashMap[Action, Double]] = MutableHashMap()

    for {
      exe <- MinimumExecutors to MaximumExecutors
      lat <- 0 until CoarseMaximumLatency
    } {
      if (lat < CoarseMinimumLatency) {
        // prefer to scale-in, in case scaleIn is not possible, do nothing
        exe match {
          case MinimumExecutors => add(space, exe, lat, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = NoReward)
          case _ => add(space, exe, lat, scaleOutReward = NoReward, noActionReward = NoReward, scaleInReward = BestReward)
        }
      } else if (lat >= CoarseMinimumLatency && lat < CoarseTargetLatency) {
        // prefer no-action
        add(space, exe, lat, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = NoReward)
      } else {
        // prefer to scale-out, in case there is not enough executors, prefer NoAction
        exe match {
          case MaximumExecutors=> add(space, exe, lat, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = NoReward)
          case _ => add(space, exe, lat, scaleOutReward = BestReward, noActionReward = NoReward, scaleInReward = NoReward)
        }
      }
    }

    new TemporalDifferenceStateSpace(space)
  }

  private def add(
                   stateSpace: MutableHashMap[State, MutableHashMap[Action, Double]],
                   numberOfExecutors: Int,
                   latency: Int,
                   scaleOutReward: Double,
                   noActionReward: Double,
                   scaleInReward: Double
                 ): Unit = {

    stateSpace += (
      State(numberOfExecutors, latency) ->
        mutable.HashMap(
          ScaleOut -> scaleOutReward,
          NoAction -> noActionReward,
          ScaleIn  -> scaleInReward
        )
      )
  }
}