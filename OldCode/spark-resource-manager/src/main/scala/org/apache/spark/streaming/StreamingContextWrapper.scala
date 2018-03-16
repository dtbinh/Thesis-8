package org.apache.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkDriverExecutionException
import org.apache.spark.streaming.scheduler.ResourceManager

/**
 * Wrapper class for creating StreamingContext with custom ResourceManager. On start it temporarily
 * overrides dynamicAllocation flag so that default ExecutorAllocatorManager is not enabled. After
 * instantiation of ResourceManager this class attaches it's listener to both Core and Streaming RPC
 * message bus.
 *
 * @param sparkContext          SparkContext instance used for StreamingContext
 * @param checkpoint            Optional checkpoint for StreamingContext restoration
 * @param batchDuration         Duration of a single micro-batch
 * @param createResourceManager Function creating new ResourceManager
 */
class StreamingContextWrapper(
    sparkContext: SparkContext,
    checkpoint: Checkpoint,
    batchDuration: Duration,
    createResourceManager: StreamingContext => ResourceManager
) extends StreamingContext(sparkContext, checkpoint, batchDuration) {
  import StreamingContextWrapper._

  val stopSparkContextByDefault: Boolean =
    conf.getBoolean(StopSparkContextByDefaultKey, defaultValue = true)

  private var resourceManager: Option[ResourceManager] = None

  def this(ssc: StreamingContext, create: StreamingContext => ResourceManager) =
    this(ssc.sparkContext, ssc.initialCheckpoint, ssc.graph.batchDuration, create)

  override def start(): Unit =
    try {
      startSteamingContext()
      startResourceManager()
    } catch {
      case e: Throwable =>
        super.stop(stopSparkContext = true, stopGracefully = false)
        throw new SparkDriverExecutionException(e)
    }

  override def stop(stopSparkContext: Boolean = stopSparkContextByDefault): Unit = {
    resourceManager.foreach(_.stop())
    super.stop(stopSparkContext)
  }

  override def stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit = {
    resourceManager.foreach(_.stop())
    super.stop(stopSparkContext, stopGracefully)
  }

  // Don't start the default ExecutorAllocationManager
  private def startSteamingContext(): Unit = {
    val flag = conf.get(DynamicResourceAllocationEnabledKey, "false")
    conf.set(DynamicResourceAllocationEnabledKey, "false")
    super.start()
    conf.set(DynamicResourceAllocationEnabledKey, flag)
  }

  @throws(classOf[IllegalArgumentException])
  @throws(classOf[IllegalStateException])
  private def startResourceManager(): Unit = {
    resourceManager = Some(createResourceManager(this))
    resourceManager foreach { manager =>
      sparkContext.addSparkListener(manager.listener)
      addStreamingListener(manager.listener)
      manager.start()
    }
  }
}

object StreamingContextWrapper {
  final val DynamicResourceAllocationEnabledKey = "spark.streaming.dynamicAllocation.enabled"
  final val StopSparkContextByDefaultKey        = "spark.streaming.stopSparkContextByDefault"
}
