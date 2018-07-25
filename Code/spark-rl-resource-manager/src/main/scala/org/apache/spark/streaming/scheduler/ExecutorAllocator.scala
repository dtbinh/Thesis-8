package org.apache.spark.streaming.scheduler

import com.sap.rl.rm.RMConstants
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{ExecutorAllocationClient, SparkException}

trait ExecutorAllocator {

  // Interface for manipulating number of executors
  protected lazy val client: ExecutorAllocationClient = streamingContext.sparkContext.schedulerBackend match {
    case backend: ExecutorAllocationClient => backend.asInstanceOf[ExecutorAllocationClient]
    case _ =>
      throw new SparkException(
        """|Dynamic resource allocation doesn't work in local mode. Please consider using
           |scheduler extending CoarseGrainedSchedulerBackend (such as Spark Standalone,
           |YARN or Mesos).""".stripMargin
      )
  }

  protected val streamingContext: StreamingContext
  protected val constants: RMConstants

  def workerExecutors: Seq[String] = activeExecutors.diff(receiverExecutors)

  def numberOfWorkerExecutors: Int = numberOfActiveExecutors - numberOfReceiverExecutors

  def numberOfActiveExecutors: Int = activeExecutors.size

  def activeExecutors: Seq[String] = client.getExecutorIds()

  def numberOfReceiverExecutors: Int = streamingContext.scheduler.receiverTracker.numReceivers()

  def receiverExecutors: Seq[String] = streamingContext.scheduler.receiverTracker.allocatedExecutors.values.flatten.toSeq

  def addExecutors(num: Int): Boolean = client.requestExecutors(num)

  def removeExecutors(executors: Seq[String]): Seq[String] = client.killExecutors(executors)

  def requestMaximumExecutors(): Unit = {
    val executorsToRequest = constants.MaximumExecutors - numberOfWorkerExecutors
    if (executorsToRequest > 0) client.requestExecutors(executorsToRequest)
  }
}
