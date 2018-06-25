package com.sap.rl.rm.td

import com.sap.rl.rm.{Action, State}
import org.apache.spark.streaming.scheduler.RMConstants

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TDStateSpace(val value: mutable.HashMap[State, ArrayBuffer[QValue]]) { }

object TDStateSpace {
  def apply(constants: RMConstants): TDStateSpace = {
    import constants._

    val space: mutable.HashMap[State, ArrayBuffer[QValue]] = mutable.HashMap()

    for {
      exe <- MinimumExecutors to MaximumExecutors
      lat <- 0 until CoarseMaximumLatency
    } {
      if (lat < CoarseMinimumLatency) {
        // prefer to scale-in
        space += (
          State(exe, lat) ->
            ArrayBuffer(
              QValue(Action.ScaleOut, 0),
              QValue(Action.NoAction, 0),
              QValue(Action.ScaleIn, 1)
            )
          )
      } else if (lat >= CoarseMinimumLatency && lat < CoarseTargetLatency) {
        // prefer no-action
        space += (
          State(exe, lat) ->
            ArrayBuffer(
              QValue(Action.ScaleOut, 0),
              QValue(Action.NoAction, 1),
              QValue(Action.ScaleIn, 0)
            )
          )
      } else {
        // prefer to scale-out
        space += (
          State(exe, lat) ->
            ArrayBuffer(
              QValue(Action.ScaleOut, 1),
              QValue(Action.NoAction, 0),
              QValue(Action.ScaleIn, 0)
            )
          )
      }
    }

    new TDStateSpace(space)
  }
}