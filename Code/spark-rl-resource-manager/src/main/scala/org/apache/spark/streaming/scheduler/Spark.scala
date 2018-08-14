package org.apache.spark.streaming.scheduler

import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext, SparkException}
import org.apache.spark.SparkContext.{RDD_SCOPE_KEY => RddScopeKey}

trait Spark {

  def streamingContext: StreamingContext
  lazy val sparkContext: SparkContext = streamingContext.sparkContext
  lazy val sparkConf: SparkConf = sparkContext.getConf
  lazy val batchDuration: Long = streamingContext.graph.batchDuration.milliseconds

  // Interface for manipulating number of executors
  private[Spark] lazy val executorAllocationClient: ExecutorAllocationClient = sparkContext.schedulerBackend match {
    case backend: ExecutorAllocationClient => backend.asInstanceOf[ExecutorAllocationClient]
    case _ =>
      throw new SparkException(
        """|Dynamic resource allocation doesn't work in local mode. Please consider using
           |scheduler extending CoarseGrainedSchedulerBackend (such as Spark Standalone,
           |YARN or Mesos).""".stripMargin
      )
  }

  // default spark dynamic executor allocator
  lazy val sparkDynamicAllocator: ExecutorAllocationManager = new ExecutorAllocationManager(executorAllocationClient,
    streamingContext.scheduler.receiverTracker,
    sparkConf,
    batchDuration,
    streamingContext.scheduler.clock)

  def numberOfActiveExecutors: Int = activeExecutors.size

  def activeExecutors: Seq[String] = executorAllocationClient.getExecutorIds()

  def requestTotalExecutors(totalExecutors: Int): Boolean = executorAllocationClient.requestTotalExecutors(totalExecutors, 0, Map.empty)

  def removeExecutors(executors: Seq[String]): Seq[String] = executorAllocationClient.killExecutors(executors)

  def batchTime(jobStart: SparkListenerJobStart): Long = {
    val pattern       = ".*\"id\":\"[0-9]+_([0-9]+)\".*".r
    val scopeProperty = jobStart.properties.getProperty(RddScopeKey)

    scopeProperty.replaceAll("\\s", "") match {
      case pattern(batchTime) => batchTime.toLong
      case _                  => throw new SparkException(s"Could not extract batch time")
    }
  }
}
