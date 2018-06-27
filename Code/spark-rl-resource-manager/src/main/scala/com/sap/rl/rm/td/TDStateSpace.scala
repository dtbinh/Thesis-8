package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Action, State}
import org.apache.spark.streaming.scheduler.RMConstants

import scala.collection.mutable

class TDStateSpace(value: mutable.HashMap[State, mutable.HashMap[Action, Double]]) {

  def updateQValueForAction(state: State, action: Action, expectedReward: Double): Unit = {
    val qValues = value(state)
    qValues(action) = expectedReward
  }

  def apply(s: State): mutable.HashMap[Action, Double] = value(s)

  def apply(s: State, a: Action): Double = value(s)(a)

  def size: Int = value.size
}

object TDStateSpace {
  def apply(constants: RMConstants): TDStateSpace = {
    import constants._

    val space: mutable.HashMap[State, mutable.HashMap[Action, Double]] = mutable.HashMap()

    for {
      exe <- MinimumExecutors to MaximumExecutors
      lat <- 0 until CoarseMaximumLatency
    } {
      if (lat < CoarseMinimumLatency) {
        // prefer to scale-in
        space += (
          State(exe, lat) ->
            mutable.HashMap(
              Action.ScaleOut -> 0,
              Action.NoAction -> 0,
              Action.ScaleIn  -> 1
            )
          )
      } else if (lat >= CoarseMinimumLatency && lat < CoarseTargetLatency) {
        // prefer no-action
        space += (
          State(exe, lat) ->
            mutable.HashMap(
              Action.ScaleOut -> 0,
              Action.NoAction -> 1,
              Action.ScaleIn  -> 0
            )
          )
      } else {
        // prefer to scale-out
        space += (
          State(exe, lat) ->
            mutable.HashMap(
              Action.ScaleOut -> 1,
              Action.NoAction -> 0,
              Action.ScaleIn  -> 0
            )
          )
      }
    }

    new TDStateSpace(space)
  }
}