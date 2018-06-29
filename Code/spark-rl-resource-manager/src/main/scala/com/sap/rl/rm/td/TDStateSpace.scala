package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Action, State}
import org.apache.spark.streaming.scheduler.RMConstants

import scala.collection.mutable

class TDStateSpace(value: mutable.HashMap[State, mutable.HashMap[Action, Double]]) {

  def updateQValueForAction(state: State, action: Action, qVal: Double): Unit = {
    val qValues = value(state)
    qValues(action) = qVal
  }

  def apply(s: State): mutable.HashMap[Action, Double] = value(s)

  def apply(s: State, a: Action): Double = value(s)(a)

  def size: Int = value.size
}

// TODO: initialize to better values for edge cases
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
              Action.ScaleOut -> NoReward,
              Action.NoAction -> NoReward,
              Action.ScaleIn  -> BestReward
            )
          )
      } else if (lat >= CoarseMinimumLatency && lat < CoarseTargetLatency) {
        // prefer no-action
        space += (
          State(exe, lat) ->
            mutable.HashMap(
              Action.ScaleOut -> NoReward,
              Action.NoAction -> BestReward,
              Action.ScaleIn  -> NoReward
            )
          )
      } else {
        // prefer to scale-out
        space += (
          State(exe, lat) ->
            mutable.HashMap(
              Action.ScaleOut -> BestReward,
              Action.NoAction -> NoReward,
              Action.ScaleIn  -> NoReward
            )
          )
      }
    }

    new TDStateSpace(space)
  }
}