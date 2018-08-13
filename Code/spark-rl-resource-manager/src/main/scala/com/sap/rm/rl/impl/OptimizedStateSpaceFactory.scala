package com.sap.rm.rl.impl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{StateSpace, StateSpaceFactory}

class OptimizedStateSpaceFactory(config: ResourceManagerConfig) extends StateSpaceFactory {
  override def initialize(space: StateSpace): StateSpace = {
    import config._

    for {
      exe <- MinimumExecutors to MaximumExecutors
      lat <- 0 until CoarseMaximumLatency
    } {
      if (lat < CoarseMinimumLatency) {
        if (exe == MinimumExecutors) {
          space.addState(exe, lat, loadIsIncreasing = false, scaleOutReward = -BestReward, noActionReward = BestReward, scaleInReward = NoReward)
          space.addState(exe, lat, loadIsIncreasing = true, scaleOutReward = -BestReward, noActionReward = BestReward, scaleInReward = NoReward)
        } else {
          space.addState(exe, lat, loadIsIncreasing = false, scaleOutReward = -BestReward, noActionReward = NoReward, scaleInReward = BestReward)
          space.addState(exe, lat, loadIsIncreasing = true, scaleOutReward = -BestReward, noActionReward = NoReward, scaleInReward = BestReward)
        }
      } else if (lat >= CoarseMinimumLatency && lat < CoarseTargetLatency) {
        if (exe == MinimumExecutors) {
          space.addState(exe, lat, loadIsIncreasing = false, scaleOutReward = -BestReward, noActionReward = BestReward, scaleInReward = NoReward)
        } else {
          space.addState(exe, lat, loadIsIncreasing = false, scaleOutReward = -BestReward, noActionReward = NoReward, scaleInReward = BestReward)
        }
        space.addState(exe, lat, loadIsIncreasing = true, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = -BestReward)
      } else {
        if (exe == MaximumExecutors) {
          space.addState(exe, lat, loadIsIncreasing = true, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = -BestReward)
        } else {
          space.addState(exe, lat, loadIsIncreasing = true, scaleOutReward = BestReward, noActionReward = NoReward, scaleInReward = -BestReward)
        }
        space.addState(exe, lat, loadIsIncreasing = false, scaleOutReward = NoReward, noActionReward = BestReward, scaleInReward = -BestReward)
      }
    }

    space
  }
}

object OptimizedStateSpaceFactory {
  def apply(config: ResourceManagerConfig): OptimizedStateSpaceFactory = new OptimizedStateSpaceFactory(config)
}
