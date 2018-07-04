package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{ExecutorAllocationClient, SparkException}

trait ExecutorAllocator {

  // Interface for manipulating number of executors
  protected lazy val executorAllocator: ExecutorAllocationClient = streamingContext.sparkContext.schedulerBackend match {
    case backend: ExecutorAllocationClient => backend
    case _ =>
      throw new SparkException(
        """|Dynamic resource allocation doesn't work in local mode. Please consider using
           |scheduler extending CoarseGrainedSchedulerBackend (such as Spark Standalone,
           |YARN or Mesos).""".stripMargin
      )
  }

  val streamingContext: StreamingContext

  def workerExecutors: Seq[String] = activeExecutors.diff(receiverExecutors)

  def numberOfWorkerExecutors: Int = numberOfActiveExecutors - numberOfReceiverExecutors

  def numberOfActiveExecutors: Int = activeExecutors.size

  def activeExecutors: Seq[String] = executorAllocator.getExecutorIds()

  def numberOfReceiverExecutors: Int = receiverExecutors.size

  def receiverExecutors: Seq[String] = streamingContext.scheduler.receiverTracker.allocatedExecutors.values.flatten.toSeq

  def addExecutors(num: Int): Boolean = executorAllocator.requestExecutors(num)

  def removeExecutors(executors: Seq[String]): Seq[String] = executorAllocator.killExecutors(executors)
}
