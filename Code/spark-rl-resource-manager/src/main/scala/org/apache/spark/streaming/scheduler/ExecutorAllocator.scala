package org.apache.spark.streaming.scheduler

import com.sap.rl.rm.Spark
import org.apache.spark.{ExecutorAllocationClient, SparkException}

trait ExecutorAllocator extends Spark {

  // Interface for manipulating number of executors
  protected lazy val client: ExecutorAllocationClient = sparkContext.schedulerBackend match {
    case backend: ExecutorAllocationClient => backend.asInstanceOf[ExecutorAllocationClient]
    case _ =>
      throw new SparkException(
        """|Dynamic resource allocation doesn't work in local mode. Please consider using
           |scheduler extending CoarseGrainedSchedulerBackend (such as Spark Standalone,
           |YARN or Mesos).""".stripMargin
      )
  }

  def numberOfActiveExecutors: Int = activeExecutors.size

  def activeExecutors: Seq[String] = client.getExecutorIds()

  def addExecutors(num: Int): Boolean = client.requestExecutors(num)

  def removeExecutors(executors: Seq[String]): Seq[String] = client.killExecutors(executors)

  def requestMaximumExecutors(maximumExecutors: Int): Unit = {
    val executorsToRequest = maximumExecutors - numberOfActiveExecutors
    if (executorsToRequest > 0) client.requestExecutors(executorsToRequest)
  }
}
