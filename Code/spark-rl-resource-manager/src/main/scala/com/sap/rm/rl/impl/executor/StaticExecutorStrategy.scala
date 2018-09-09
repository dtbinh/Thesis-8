package com.sap.rm.rl.impl.executor

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.ExecutorStrategy

class StaticExecutorStrategy(config: ResourceManagerConfig) extends ExecutorStrategy {
  import config._

  override def howManyExecutorsToAdd(totalExecutors: Int): Int = ExecutorGranularity

  override def howManyExecutorsToRemove(totalExecutors: Int): Int = ExecutorGranularity
}

object StaticExecutorStrategy {
  def apply(config: ResourceManagerConfig): StaticExecutorStrategy = new StaticExecutorStrategy(config)
}