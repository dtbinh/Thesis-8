package org.apache.spark.streaming.scheduler

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext, SparkException}

abstract class ResourceManager(constants: RMConstants, streamingContext: StreamingContext) extends Logging {

  val sparkConf: SparkConf = streamingContext.conf
  val sparkContext: SparkContext = streamingContext.sparkContext
  val batchDuration: Long = streamingContext.graph.batchDuration.milliseconds

  // Interface for manipulating number of executors
  protected lazy val executorAllocator: ExecutorAllocationClient = sparkContext.schedulerBackend match {
    case backend: ExecutorAllocationClient => backend
    case _ =>
      throw new SparkException(
        """|Dynamic resource allocation doesn't work in local mode. Please consider using
           |scheduler extending CoarseGrainedSchedulerBackend (such as Spark Standalone,
           |YARN or Mesos).""".stripMargin
      )
  }

  // default listening infrastructure for spark-streaming
  val listener: StreamingListener = new BatchListener { }

  def start(): Unit = log.info("Started resource manager")

  def stop(): Unit = log.info("Stopped resource manager")

  def workerExecutors: Int = activeExecutors - receiverExecutors

  def activeExecutors: Int = executorAllocator.getExecutorIds().size

  def receiverExecutors: Int = streamingContext.scheduler.receiverTracker.allocatedExecutors.values.flatten.toSeq.size
}