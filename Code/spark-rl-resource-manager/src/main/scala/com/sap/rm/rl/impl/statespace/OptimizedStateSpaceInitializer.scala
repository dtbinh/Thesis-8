package com.sap.rm.rl.impl.statespace

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{StateSpace, StateSpaceInitializer}

class OptimizedStateSpaceInitializer(config: ResourceManagerConfig) extends StateSpaceInitializer {
  import config._

  override def initialize(space: StateSpace): StateSpace = {
    for {
      lat <- 0 until CoarseMaximumLatency
    } {
      if (lat < CoarseTargetLatency) {
        val normalizedLatency = normalize(lat)
        //space.addState(lat, loadIsIncreasing = true, scaleOutReward = -BestReward, noActionReward = normalizedLatency, scaleInReward = 1 - normalizedLatency)
        space.addState(lat, loadIsIncreasing = true, scaleOutReward = normalizedLatency, noActionReward = 1 - normalizedLatency, scaleInReward = NoReward)
        space.addState(lat, loadIsIncreasing = false, scaleOutReward = NoReward, noActionReward = normalizedLatency, scaleInReward = 1 - normalizedLatency)
      } else {
        space.addState(lat, loadIsIncreasing = true, scaleOutReward = BestReward, noActionReward = NoReward, scaleInReward = -BestReward)
        space.addState(lat, loadIsIncreasing = false, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = -BestReward)
      }
    }

    space
  }

  private def normalize(latency: Int): Double = latency.toDouble / CoarseTargetLatency
}

object OptimizedStateSpaceInitializer {
  def apply(config: ResourceManagerConfig): OptimizedStateSpaceInitializer = new OptimizedStateSpaceInitializer(config)
}
