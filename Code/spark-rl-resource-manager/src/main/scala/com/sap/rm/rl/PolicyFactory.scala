package com.sap.rm.rl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.impl.DefaultRandomNumberGenerator
import com.sap.rm.rl.impl.policy.{DecreasingOneMinusEpsilonPolicy, GreedyPolicy, OneMinusEpsilonPolicy}

object PolicyFactory {
  def getPolicy(config: ResourceManagerConfig): Policy = {
    config.Policy match {
      case "greedy" => GreedyPolicy(config)
      case "oneMinusEpsilon" => OneMinusEpsilonPolicy(config, GreedyPolicy(config), DefaultRandomNumberGenerator(config.Seed))
      case "decreasingOneMinusEpsilon" => DecreasingOneMinusEpsilonPolicy(config, GreedyPolicy(config), DefaultRandomNumberGenerator(config.Seed))
    }
  }
}
