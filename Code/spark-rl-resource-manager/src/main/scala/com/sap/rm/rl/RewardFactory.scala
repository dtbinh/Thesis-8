package com.sap.rm.rl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.impl.reward.{PreferNoActionWhenLoadIsDecreasing, PreferScaleInWhenLoadIsDescreasing}

object RewardFactory {
  def getReward(config: ResourceManagerConfig): Reward = {
    config.Reward match {
      case "preferScaleInWhenLoadIsDescreasing" => PreferScaleInWhenLoadIsDescreasing(config)
      case "preferNoActionWhenLoadIsDecreasing" => PreferNoActionWhenLoadIsDecreasing(config)
    }
  }
}
