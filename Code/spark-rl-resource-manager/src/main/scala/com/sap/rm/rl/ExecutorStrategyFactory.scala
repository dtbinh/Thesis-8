package com.sap.rm.rl

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.impl.executor.{LinearExecutorStrategy, StaticExecutorStrategy}

object ExecutorStrategyFactory {
  def getExecutorStrategy(config: ResourceManagerConfig): ExecutorStrategy = {
    config.ExecutorStrategy match {
      case "static" => StaticExecutorStrategy(config)
      case "linear" => LinearExecutorStrategy(config)
    }
  }
}
