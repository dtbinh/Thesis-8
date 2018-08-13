package com.sap.rm.rl

import com.sap.rm.ResourceManagerConfig
import Action._

import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}

@SerialVersionUID(1L)
class StateSpace(value: MutableHashMap[State, MutableHashMap[Action, Double]]) extends Serializable {

  def updateQValueForAction(state: State, action: Action, qVal: Double): Unit = {
    val qValues = value(state)
    qValues(action) = qVal
  }

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

  def size: Int = value.size
}

object StateSpace {
  def apply(config: ResourceManagerConfig): StateSpace = {
    config.InitializationMode match {
      case "optimal" => optimalInitializer(config)
      case "zero" => zeroInitializer(config)
      case "random" => randomInitializer(config)
    }
  }

  private def zeroInitializer(config: ResourceManagerConfig): StateSpace = {
    import config._

    val space: MutableHashMap[State, MutableHashMap[Action, Double]] = MutableHashMap()

    for {
      exe <- MinimumExecutors to MaximumExecutors
      lat <- 0 until CoarseMaximumLatency
      loadIsIncreasing <- List(false, true)
    } {
      // zero out everything
      add(space, exe, lat, loadIsIncreasing, scaleOutReward = NoReward, noActionReward = NoReward, scaleInReward = NoReward)
    }

    new StateSpace(space)
  }

  private def randomInitializer(config: ResourceManagerConfig): StateSpace = {
    import config._

    val space: MutableHashMap[State, MutableHashMap[Action, Double]] = MutableHashMap()

    val rand = new scala.util.Random(System.currentTimeMillis())
    for {
      exe <- MinimumExecutors to MaximumExecutors
      lat <- 0 until CoarseMaximumLatency
      loadIsIncreasing <- List(false, true)
    } {
      val scaleOutReward = (rand.nextDouble() * 2) - 1
      val noActionReward = (rand.nextDouble() * 2) - 1
      val scaleInReward  = (rand.nextDouble() * 2) - 1
      add(space, exe, lat, loadIsIncreasing, scaleOutReward, noActionReward, scaleInReward)
    }

    new StateSpace(space)
  }

  private def optimalInitializer(config: ResourceManagerConfig): StateSpace = {
    import config._

    val space: MutableHashMap[State, MutableHashMap[Action, Double]] = MutableHashMap()

    for {
      exe <- MinimumExecutors to MaximumExecutors
      lat <- 0 until CoarseMaximumLatency
    } {
      if (lat < CoarseMinimumLatency) {
        if (exe == MinimumExecutors) {
          add(space, exe, lat, loadIsIncreasing = false, scaleOutReward = -BestReward, noActionReward = BestReward, scaleInReward = NoReward)
          add(space, exe, lat, loadIsIncreasing = true, scaleOutReward = -BestReward, noActionReward = BestReward, scaleInReward = NoReward)
        } else {
          add(space, exe, lat, loadIsIncreasing = false, scaleOutReward = -BestReward, noActionReward = NoReward, scaleInReward = BestReward)
          add(space, exe, lat, loadIsIncreasing = true, scaleOutReward = -BestReward, noActionReward = NoReward, scaleInReward = BestReward)
        }
      } else if (lat >= CoarseMinimumLatency && lat < CoarseTargetLatency) {
        if (exe == MinimumExecutors) {
          add(space, exe, lat, loadIsIncreasing = false, scaleOutReward = -BestReward, noActionReward = BestReward, scaleInReward = NoReward)
        } else {
          add(space, exe, lat, loadIsIncreasing = false, scaleOutReward = -BestReward, noActionReward = NoReward, scaleInReward = BestReward)
        }
        add(space, exe, lat, loadIsIncreasing = true, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = -BestReward)
      } else {
        if (exe == MaximumExecutors) {
          add(space, exe, lat, loadIsIncreasing = true, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = -BestReward)
        } else {
          add(space, exe, lat, loadIsIncreasing = true, scaleOutReward = BestReward, noActionReward = NoReward, scaleInReward = -BestReward)
        }
        add(space, exe, lat, loadIsIncreasing = false, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = -BestReward)
      }
    }

    new StateSpace(space)
  }

  private def add(
                   stateSpace: MutableHashMap[State, MutableHashMap[Action, Double]],
                   numberOfExecutors: Int,
                   latency: Int,
                   loadIsIncreasing: Boolean,
                   scaleOutReward: Double,
                   noActionReward: Double,
                   scaleInReward: Double
                 ): Unit = {

    stateSpace += (
      State(numberOfExecutors, latency, loadIsIncreasing) ->
        mutable.HashMap(
          ScaleOut -> scaleOutReward,
          NoAction -> noActionReward,
          ScaleIn -> scaleInReward
        )
      )
  }
}