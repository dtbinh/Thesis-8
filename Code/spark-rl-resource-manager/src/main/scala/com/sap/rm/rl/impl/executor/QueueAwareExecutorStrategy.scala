package com.sap.rm.rl.impl.executor

import java.lang.Math.max

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{ExecutorStrategy, TemporalDifferenceResourceManager}

class QueueAwareExecutorStrategy(config: ResourceManagerConfig, rm: TemporalDifferenceResourceManager) extends ExecutorStrategy {

  import config._
  import rm._

  override def howManyExecutorsToAdd(totalExecutors: Int): Int = {
    //val diff = max(ceil(waitingListLength.toDouble / targetWaitingListLength), 1)
    val diff = max(waitingListLength - targetWaitingListLength, 1)
    if (diff + totalExecutors > MaximumExecutors) MaximumExecutors - totalExecutors else diff.toInt
  }

  override def howManyExecutorsToRemove(totalExecutors: Int): Int = ExecutorGranularity
}

object QueueAwareExecutorStrategy {
  def apply(config: ResourceManagerConfig, rm: TemporalDifferenceResourceManager): QueueAwareExecutorStrategy = new QueueAwareExecutorStrategy(config, rm)
}