package org.apache.spark.streaming.scheduler

import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext, SparkException}

abstract class ResourceManager(constants: RMConstants, streamingContext: StreamingContext) extends StreamingListener {

  @transient private lazy val log = LogManager.getLogger(this.getClass)

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

  def start(): Unit = log.info("Started resource manager")

  def stop(): Unit = log.info("Stopped resource manager")

  def numberOfWorkerExecutors: Int = numberOfActiveExecutors - numberOfReceiverExecutors
  def workerExecutors: Seq[String] = activeExecutors.diff(receiverExecutors)

  def numberOfActiveExecutors: Int = activeExecutors.size
  def activeExecutors: Seq[String] = executorAllocator.getExecutorIds()

  def numberOfReceiverExecutors: Int = receiverExecutors.size
  def receiverExecutors: Seq[String] = streamingContext.scheduler.receiverTracker.allocatedExecutors.values.flatten.toSeq
}