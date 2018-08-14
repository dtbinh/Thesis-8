package com.sap.rm.rl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.impl.{OptimizedStateSpaceInitializer, RandomStateSpaceInitializer, ZeroStateSpaceInitializer}

trait StateSpaceInitializer {
  def initialize(space: StateSpace): StateSpace
}

object StateSpaceInitializer {
  def getInstance(config: ResourceManagerConfig): StateSpaceInitializer = {
    config.InitializationMode match {
      case "optimal" => OptimizedStateSpaceInitializer(config)
      case "zero" => ZeroStateSpaceInitializer(config)
      case "random" => RandomStateSpaceInitializer(config)
    }
  }
}
