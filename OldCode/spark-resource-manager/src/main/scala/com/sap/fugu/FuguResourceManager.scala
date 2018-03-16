package com.sap.fugu

import com.sap.fugu.optimizer.OptimizerResult
import com.sap.fugu.optimizer.ResourceOptimizer
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.ResourceManager
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.spark.streaming.scheduler.listener.BatchListener
import org.apache.spark.streaming.scheduler.listener.model.BatchTier

import scala.math.abs
import scala.math.ceil
import scala.math.max
import scala.math.min
import scala.math.round
import scala.math.signum
import scala.util.Random.shuffle

/**
 * Main ResourceManager implementation based on ideas lying under original FUGU stream processing
 * engine. It works with with following additional parameters:
 *   - thresholdBreaches:   Range of breaches allowed before optimizer module takes action. Exact
 *                          value is determined by length of batch history
 *   - adaptiveWindow.delta Delta value for AdaptiveWindow algorithm. The lower the value, the
 *                          longer utilization history will be remembered.
 *   - backupExecutors      Number of executors to be kept ahead of optimal number in order to
 *                          make executor addition process milder.
 * Important assumption here is that a single executor corresponds to execution of a single task in
 * heterogeneous cluster system.
 *
 * Internally it utilizes BatchListener along with ResourceOptimizer for event registration and
 * resource optimization respectively.
 *
 * @param streamingContext StreamingContext of the application
 * @param bufferSize       Limit for number of remembered batches
 */
class FuguResourceManager(streamingContext: StreamingContext, val bufferSize: Int = 1024)
    extends ResourceManager(streamingContext) {
  import FuguResourceManager._

  val coresPerTask: Int     = sparkConf.getInt(CoresPerTaskKey, CoresPerTaskDefault)
  val coresPerExecutor: Int = sparkConf.getInt(CoresPerExecutorKey, CoresPerExecutorDefault)

  val thresholdBreaches: Range =
    parseConfigRange(sparkConf.get(ThresholdBreachesKey, ThresholdBreachesDefault))

  val adaptiveWindowDelta: Double =
    sparkConf.getDouble(AdaptiveWindowDeltaKey, AdaptiveWindowDeltaDefault)

  val backupExecutors: Int =
    sparkConf.getInt(BackupExecutorsKey, ceil(0.1 * maximumExecutors).toInt)

  val optimizer = new ResourceOptimizer(this)

  override val listener: BatchListener = new BatchListener() {
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      super.onBatchCompleted(batchCompleted)

      if (batches.size > max(thresholdBreaches.head + thresholdBreaches.step * 2, 16)) {
        val result = adapt(batches)
        log.debug(s"Cluster state: ${getInfo(batches.last)},$result")
      } else {
        log.debug(s"Cluster state: ${getInfo(batches.last)}")
      }
    }
  }

  private var lastOptimizationState = OptimizationState(0, 0)

  validateSettings()

  override def start(): Unit = {
    super.start()
    listener.bufferSize = Some(bufferSize)
  }

  private def adapt(batches: Seq[BatchTier]): OptimizerResult = synchronized {
    val result            = optimizer.calculateExecutors(batches)
    val executorNo        = executors.size
    val optimalExecutorNo = result.executorNo
    val change            = calculateChange(optimalExecutorNo)
    val state             = OptimizationState(executorNo, optimalExecutorNo)
    log.info(s"Optimal executor no is $optimalExecutorNo, expecting ($executorNo + $change)")

    if (lastOptimizationState != state) {
      val reconfigured = (change == 0) || (change != 0 && reconfigure(change))

      if (reconfigured) {
        if (change != 0) log.info("Cluster reconfigured")
        lastOptimizationState = state
      }
    }

    result
  }

  private def getInfo(batch: BatchTier): String = {
    val duration   = batch.duration
    val delay      = batch.delay
    val load       = batch.records
    val executorNo = executors.size

    s"$duration,$delay,$load,$executorNo"
  }

  // Number of executors to change is determined with overhead based on granularity
  private def calculateChange(proposed: Int): Int = {
    val diff     = min(proposed + backupExecutors, maximumExecutors) - executors.size
    val softDiff = round((abs(diff) + (executorGranularity - 1) * min(signum(diff), 0)) / 2.0)

    signum(diff) * ceil(softDiff.toDouble / executorGranularity).toInt * executorGranularity
  }

  // Executors for removal are selected at random
  private def reconfigure(change: Int): Boolean = {
    log.debug(s"Asking for $change executors")

    if (change < 0) {
      executorAllocator.killExecutors(shuffle(workingExecutors).take(-change)).nonEmpty
    } else {
      executorAllocator.requestExecutors(change)
    }
  }

  private def validateSettings(): Unit = {
    require(coresPerExecutor == coresPerTask)

    require(minimumExecutors > 1)
    require(backupExecutors >= 0)

    require(adaptiveWindowDelta > 0.0)
    require(adaptiveWindowDelta <= 1.0)

    require(thresholdBreaches.start >= 0)
    require(thresholdBreaches.nonEmpty)
    require(thresholdBreaches.step > 0)
  }

  private case class OptimizationState(executorNo: Int, optimalExecutorNo: Int)
}

object FuguResourceManager {
  final val CoresPerTaskKey     = "spark.task.cpus"
  final val CoresPerTaskDefault = 1

  final val CoresPerExecutorKey     = "spark.executor.cores"
  final val CoresPerExecutorDefault = 0

  final val ThresholdBreachesKey     = "spark.streaming.dynamicAllocation.thresholdBreaches"
  final val ThresholdBreachesDefault = "2 to 256 by 4"

  final val AdaptiveWindowDeltaKey     = "spark.streaming.dynamicAllocation.adaptiveWindow.delta"
  final val AdaptiveWindowDeltaDefault = 0.5

  final val BackupExecutorsKey = "spark.streaming.dynamicAllocation.backupExecutors"

  def parseConfigRange(range: String): Range = {
    val pattern = "(\\d\\.?\\d*)to(\\d\\.?\\d*)by(\\d\\.?\\d*)".r

    range.replaceAll("\\s", "").toLowerCase match {
      case pattern(lower, upper, step) => lower.toInt to upper.toInt by step.toInt
      case _                           => throw new SparkException("Cannot parse config range")
    }
  }
}
