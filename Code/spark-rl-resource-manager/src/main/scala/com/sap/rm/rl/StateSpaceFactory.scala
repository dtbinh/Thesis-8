package com.sap.rm.rl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.impl.{OptimizedStateSpaceFactory, RandomStateSpaceFactory, ZeroStateSpaceFactory}

trait StateSpaceFactory {
  def initialize(space: StateSpace): StateSpace
}

object StateSpaceFactory {
  def factoryInstance(config: ResourceManagerConfig): StateSpaceFactory = {
    config.InitializationMode match {
      case "optimal" => OptimizedStateSpaceFactory(config)
      case "zero" => ZeroStateSpaceFactory(config)
      case "random" => RandomStateSpaceFactory(config)
    }
  }
}
