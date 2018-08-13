package com.sap.rm.rl.impl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{StateSpace, StateSpaceFactory}

class ZeroStateSpaceFactory(config: ResourceManagerConfig) extends StateSpaceFactory {
  override def initialize(space: StateSpace): StateSpace = {
    import config._

    for {
      exe <- MinimumExecutors to MaximumExecutors
      lat <- 0 until CoarseMaximumLatency
      loadIsIncreasing <- List(false, true)
    } {
      // zero out everything
      space.addState(exe, lat, loadIsIncreasing, scaleOutReward = NoReward, noActionReward = NoReward, scaleInReward = NoReward)
    }

    space
  }
}

object ZeroStateSpaceFactory {
  def apply(config: ResourceManagerConfig): ZeroStateSpaceFactory = new ZeroStateSpaceFactory(config)
}