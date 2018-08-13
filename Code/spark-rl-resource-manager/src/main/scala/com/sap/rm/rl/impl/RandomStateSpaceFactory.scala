package com.sap.rm.rl.impl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{StateSpace, StateSpaceFactory}

class RandomStateSpaceFactory(config: ResourceManagerConfig) extends StateSpaceFactory {
  override def initialize(space: StateSpace): StateSpace = {
    import config._

    val rand = new scala.util.Random(System.currentTimeMillis())
    for {
      exe <- MinimumExecutors to MaximumExecutors
      lat <- 0 until CoarseMaximumLatency
      loadIsIncreasing <- List(false, true)
    } {
      val scaleOutReward = (rand.nextDouble() * 2) - 1
      val noActionReward = (rand.nextDouble() * 2) - 1
      val scaleInReward = (rand.nextDouble() * 2) - 1
      space.addState(exe, lat, loadIsIncreasing, scaleOutReward, noActionReward, scaleInReward)
    }

    space
  }
}

object RandomStateSpaceFactory {
  def apply(config: ResourceManagerConfig): RandomStateSpaceFactory = new RandomStateSpaceFactory(config)
}
