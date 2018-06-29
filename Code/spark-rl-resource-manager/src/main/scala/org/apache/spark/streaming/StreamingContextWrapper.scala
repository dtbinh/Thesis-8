package org.apache.spark.streaming

import org.apache.spark.streaming.StreamingContextWrapper.{DynamicResourceAllocationEnabledKey, StopSparkContextByDefaultKey}
import org.apache.spark.streaming.scheduler.ResourceManager
import org.apache.spark.{SparkContext, SparkDriverExecutionException}

class StreamingContextWrapper(
                               sparkContext: SparkContext, checkpoint: Checkpoint,
                               batchDuration: Duration,
                               createResourceManager: StreamingContext => ResourceManager
                             ) extends StreamingContext(sparkContext, checkpoint, batchDuration) {

  val stopSparkContextByDefault: Boolean = conf.getBoolean(StopSparkContextByDefaultKey, defaultValue = true)

  private var resourceManager: ResourceManager = _

  def this(ssc: StreamingContext, create: StreamingContext => ResourceManager) =
    this(ssc.sparkContext, ssc.initialCheckpoint, ssc.graph.batchDuration, create)

  override def start(): Unit = {
    try {
      startSteamingContext()
      startResourceManager()
    } catch {
      case e: Throwable =>
        super.stop(stopSparkContext = true, stopGracefully = false)
        throw new SparkDriverExecutionException(e)
    }
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
    resourceManager = createResourceManager(this)
    addStreamingListener(resourceManager)
    resourceManager.start()
  }

  override def stop(stopSparkContext: Boolean = stopSparkContextByDefault): Unit = {
    resourceManager.stop()
    super.stop(stopSparkContext)
  }

  override def stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit = {
    resourceManager.stop()
    super.stop(stopSparkContext, stopGracefully)
  }
}

object StreamingContextWrapper {
  final val DynamicResourceAllocationEnabledKey = "spark.streaming.dynamicAllocation.enabled"
  final val StopSparkContextByDefaultKey = "spark.streaming.stopSparkContextByDefault"
}