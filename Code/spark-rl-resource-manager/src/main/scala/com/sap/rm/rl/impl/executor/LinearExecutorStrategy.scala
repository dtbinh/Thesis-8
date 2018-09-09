package com.sap.rm.rl.impl.executor

import java.lang.Math.min

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.ExecutorStrategy

class LinearExecutorStrategy(config: ResourceManagerConfig) extends ExecutorStrategy {

  private var executorsAddCounter: Int = 1
  private var executorsRemoveCounter: Int = 1

  import config._

  override def howManyExecutorsToAdd(totalExecutors: Int): Int = {
    executorsRemoveCounter = 1
    val toAdd = ExecutorGranularity * executorsAddCounter
    executorsAddCounter += 1

    min(toAdd, MaximumExecutors - totalExecutors)
  }

  override def howManyExecutorsToRemove(totalExecutors: Int): Int = {
    executorsAddCounter = 1
    val toRemove = ExecutorGranularity * executorsRemoveCounter
    executorsRemoveCounter += 1

    min(toRemove, totalExecutors - MinimumExecutors)
  }
}

object LinearExecutorStrategy {
  def apply(config: ResourceManagerConfig): LinearExecutorStrategy = new LinearExecutorStrategy(config)
}