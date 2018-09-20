package com.sap.rm.rl.impl.statespace

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{StateSpace, StateSpaceInitializer}

class ZeroStateSpaceInitializer(config: ResourceManagerConfig) extends StateSpaceInitializer {
  override def initialize(space: StateSpace): StateSpace = {
    import config._

    for {
      lat <- 0 until CoarseMaximumLatency
      loadIsIncreasing <- List(false, true)
    } {
      // zero out everything
      space.addState(lat, loadIsIncreasing, scaleOutReward = NoReward, noActionReward = NoReward, scaleInReward = NoReward)
    }

    space
  }
}

object ZeroStateSpaceInitializer {
  def apply(config: ResourceManagerConfig): ZeroStateSpaceInitializer = new ZeroStateSpaceInitializer(config)
}