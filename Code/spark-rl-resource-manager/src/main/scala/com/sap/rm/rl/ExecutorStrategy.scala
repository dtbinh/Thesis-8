package com.sap.rm.rl

trait ExecutorStrategy {
  def howManyExecutorsToAdd(totalExecutors: Int): Int
  def howManyExecutorsToRemove(totalExecutors: Int): Int
}
