package com.sap.rm.rl

import scala.collection.mutable.{HashSet => MutableHashSet, Set => MutableSet}

class BatchWaitingList(batchWaitingListThreshold: Int) {

  private lazy val waitingList: MutableSet[Long] = MutableHashSet()
  private var lastWaitingBatches: Int = 0

  def enqueue(batchSubmissionTime: Long): Unit = waitingList += batchSubmissionTime

  def dequeue(batchSubmissionTime: Long): Unit = waitingList.remove(batchSubmissionTime)

  def reset(): Unit = lastWaitingBatches = waitingList.size

  def length: Int = waitingList.size

  def isGrowing: Boolean = waitingList.size >= batchWaitingListThreshold && waitingList.size >= lastWaitingBatches

  def isShrinking: Boolean = waitingList.size < lastWaitingBatches
}

object BatchWaitingList {
  def apply(batchWaitingListThreshold: Int): BatchWaitingList = new BatchWaitingList(batchWaitingListThreshold)
}
