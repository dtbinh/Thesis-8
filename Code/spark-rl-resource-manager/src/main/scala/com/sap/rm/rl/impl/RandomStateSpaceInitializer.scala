package com.sap.rm.rl.impl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{StateSpace, StateSpaceInitializer}

class RandomStateSpaceInitializer(config: ResourceManagerConfig) extends StateSpaceInitializer {
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

object RandomStateSpaceInitializer {
  def apply(config: ResourceManagerConfig): RandomStateSpaceInitializer = new RandomStateSpaceInitializer(config)
}
