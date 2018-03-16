package org.apache.spark.streaming.scheduler

import org.apache.spark.ExecutorAllocationClient
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.ExecutorAllocationManager.isDynamicAllocationEnabled
import org.apache.spark.streaming.scheduler.listener.SparkListenerT

/**
 * Base class for creating custom resource managers. It provides interface for listening to Spark
 * events, monitoring resource usage and manipulating number of executors. Mandatory parameters that
 * should be included in config consist of:
 *  - minExecutors: Minimal number of executors that are to be used in this cluster
 *  - maxExecutors: Maximal number of executors that are to be used in this cluster
 *  - granularity:  The minimal number of executors to add or remove. No action should be taken for
 *                  lower values
 *
 * @param streamingContext StreamingContext of the application
 */
abstract class ResourceManager(streamingContext: StreamingContext) extends Logging {
  import ResourceManager._

  val sparkConf: SparkConf       = streamingContext.conf
  val sparkContext: SparkContext = streamingContext.sparkContext
  val batchDuration: Long        = streamingContext.graph.batchDuration.milliseconds

  val minimumExecutors: Int = sparkConf.getInt(MinimumExecutorsKey, MinimumExecutorsDefault)
  val maximumExecutors: Int = sparkConf.getInt(MaximumExecutorsKey, MaximumExecutorsDefault)

  val executorGranularity: Int =
    sparkConf.getInt(ExecutorGranularityKey, ExecutorGranularityDefault)

  // Overridable listeners union providing listening capabilities for Core and Streaming RPC calls
  val listener: SparkListenerT with StreamingListener = new SparkListenerT with StreamingListener {}

  validateSettings()

  /**
   * Interface for manipulating number of executors
   */
  protected lazy val executorAllocator: ExecutorAllocationClient =
    sparkContext.schedulerBackend match {
      case backend: ExecutorAllocationClient => backend
      case _ =>
        throw new SparkException(
          """|Dynamic resource allocation doesn't work in local mode. Please consider using
             |scheduler extending CoarseGrainedSchedulerBackend (such as Spark Standalone,
             |YARN or Mesos).""".stripMargin
        )
    }

  def start(): Unit = log.info("Started resource manager")
  def stop(): Unit  = log.info("Stopped resource manager")

  def executors: Seq[String]        = executorAllocator.getExecutorIds
  def receivers: Seq[String]        = receiverTracker.allocatedExecutors.values.flatten.toSeq
  def workingExecutors: Seq[String] = executors.diff(receivers)

  private def receiverTracker: ReceiverTracker = streamingContext.scheduler.receiverTracker

  private def validateSettings(): Unit = {
    require(isDynamicAllocationEnabled(sparkConf))

    require(maximumExecutors >= minimumExecutors)
    require(minimumExecutors > 0)

    require(executorGranularity > 0)
  }
}

object ResourceManager {
  final val MinimumExecutorsKey     = "spark.streaming.dynamicAllocation.minExecutors"
  final val MinimumExecutorsDefault = 1

  final val MaximumExecutorsKey     = "spark.streaming.dynamicAllocation.maxExecutors"
  final val MaximumExecutorsDefault = Int.MaxValue

  final val ExecutorGranularityKey     = "spark.streaming.dynamicAllocation.granularity"
  final val ExecutorGranularityDefault = 2
}
