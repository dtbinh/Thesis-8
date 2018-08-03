package com.sap.rm.fugu

import com.sap.rm.fugu.model.{BatchTier, JobTier, StageTier, TaskTier}
import com.sap.rm.fugu.optimizer.{OptimizerResult, ResourceOptimizer}
import com.sap.rm.{ResourceManager, ResourceManagerConfig}
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkException
import org.apache.spark.scheduler._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

import scala.collection.mutable
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
 * @param bufSize       Limit for number of remembered batches
 */
class FuguResourceManager(val config: ResourceManagerConfig, val streamingContext: StreamingContext, val bufferSize: Int = 1024) extends ResourceManager {
  import FuguResourceManager._
  import config._

  @transient private[FuguResourceManager] lazy val log: Logger = Logger(classOf[FuguResourceManager])

  val thresholdBreaches: Range = parseConfigRange(sparkConf.get(ThresholdBreachesKey, ThresholdBreachesDefault))
  val adaptiveWindowDelta: Double = sparkConf.getDouble(AdaptiveWindowDeltaKey, AdaptiveWindowDeltaDefault)

  val optimizer = new ResourceOptimizer(this)
  private val batchQueue = new mutable.Queue[BatchTier]()

  private val batchIdToJobs  = new mutable.HashMap[Long, List[JobTier]]()
  private val jobIdToStages  = new mutable.HashMap[Long, List[StageTier]]()
  private val stageIdToTasks = new mutable.HashMap[Long, List[TaskTier]]()

  // Help with finding relations between entities. It's a problem related to original models.
  private val stageIdToJobId  = new mutable.HashMap[Long, Long]()
  private val jobIdToJobStart = new mutable.HashMap[Long, SparkListenerJobStart]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    jobStart.stageIds.map(_.toLong).foreach(stageIdToJobId(_) = jobStart.jobId)
    jobIdToJobStart(jobStart.jobId) = jobStart
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    if (info.successful) addTask(info, taskEnd.stageId)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    val info       = stageCompleted.stageInfo
    val isFinished = info.completionTime.nonEmpty && info.failureReason.isEmpty

    stageIdToJobId.remove(info.stageId) match {
      case Some(jobId) => if (isFinished) addStage(stageCompleted.stageInfo, jobId)
      case None        => log.error(s"Can't register stage ${info.stageId}, unknown job id")
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    if (jobEnd.jobResult == JobSucceeded) addJob(jobEnd.jobId, jobEnd.time)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    val info      = batchCompleted.batchInfo
    if (processBatch(info)) {
      val batchTime = info.batchTime.milliseconds
      val batch     = BatchTier(info, pollJobs(batchTime))

      if (bufferSize == batchQueue.size) batchQueue.dequeue()
      batchQueue.enqueue(batch)

      if (batchQueue.size > max(thresholdBreaches.head + thresholdBreaches.step * 2, 16)) {
        val result = adapt(batchQueue)
        log.info(s"Cluster state: ${getInfo(batchQueue.last)},$result")
      } else {
        log.debug(s"Cluster state: ${getInfo(batchQueue.last)}")
      }
    }
  }

  private def addTask(info: TaskInfo, stageId: Long): Unit = {
    val task = TaskTier(info, stageId)
    stageIdToTasks(stageId) = stageIdToTasks.getOrElse(stageId, Nil) ::: task :: Nil
  }

  private def addStage(info: StageInfo, jobId: Long): Unit = {
    val stage = StageTier(info, jobId, pollTasks(info.stageId))
    jobIdToStages(jobId) = jobIdToStages.getOrElse(jobId, Nil) ::: stage :: Nil
  }

  private def addJob(jobId: Long, stopTime: Long): Unit = {
    jobIdToJobStart.remove(jobId) match {
      case Some(jobStart) => addJob(jobStart, stopTime, batchTime(jobStart))
      case None           => log.error(s"Could not update job $jobId, unknown start")
    }
  }

  private def pollJobs(batchId: Long): Seq[JobTier] = {
    batchIdToJobs.remove(batchId).getOrElse(Nil).sorted
  }

  private def pollTasks(stageId: Long): Seq[TaskTier] = {
    stageIdToTasks.remove(stageId).getOrElse(Nil).sorted
  }

  private def addJob(start: SparkListenerJobStart, stopTime: Long, batchId: Long): Unit = {
    val job = JobTier(start, stopTime, batchId, pollStages(start.jobId))
    batchIdToJobs(batchId) = batchIdToJobs.getOrElse(batchId, Nil) ::: job :: Nil
  }

  private def pollStages(jobId: Long): Seq[StageTier] = {
    jobIdToStages.remove(jobId).getOrElse(Nil).sorted
  }

  private var lastOptimizationState = OptimizationState(0, 0)

  validateSettings()

  private def adapt(batches: Seq[BatchTier]): OptimizerResult = synchronized {
    val result            = optimizer.calculateExecutors(batches)
    val executorNo        = numberOfActiveExecutors
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
    val executorNo = numberOfActiveExecutors

    s"$duration,$delay,$load,$executorNo"
  }

  // Number of executors to change is determined with overhead based on granularity
  private def calculateChange(proposed: Int): Int = {
    val diff     = min(proposed, MaximumExecutors) - numberOfActiveExecutors
    val softDiff = round((abs(diff) + (ExecutorGranularity - 1) * min(signum(diff), 0)) / 2.0)

    signum(diff) * ceil(softDiff.toDouble / ExecutorGranularity).toInt * ExecutorGranularity
  }

  // Executors for removal are selected at random
  private def reconfigure(change: Int): Boolean = {
    log.info(s"Asking for $change executors")

    if (change < 0) {
      removeExecutors(shuffle(activeExecutors).take(-change)).nonEmpty
    } else {
      requestTotalExecutors(min(numberOfActiveExecutors + change, MaximumExecutors))
    }
  }

  private def validateSettings(): Unit = {
    require(adaptiveWindowDelta > 0.0)
    require(adaptiveWindowDelta <= 1.0)

    require(thresholdBreaches.start >= 0)
    require(thresholdBreaches.nonEmpty)
    require(thresholdBreaches.step > 0)
  }

  private case class OptimizationState(executorNo: Int, optimalExecutorNo: Int)
}

object FuguResourceManager {

  final val ThresholdBreachesKey     = "spark.streaming.dynamicAllocation.thresholdBreaches"
  final val ThresholdBreachesDefault = "2 to 256 by 4"

  final val AdaptiveWindowDeltaKey     = "spark.streaming.dynamicAllocation.adaptiveWindow.delta"
  final val AdaptiveWindowDeltaDefault = 0.5

  def apply(config: ResourceManagerConfig, ssc: StreamingContext): FuguResourceManager = {
    new FuguResourceManager(config, ssc)
  }

  def parseConfigRange(range: String): Range = {
    val pattern = "(\\d\\.?\\d*)to(\\d\\.?\\d*)by(\\d\\.?\\d*)".r

    range.replaceAll("\\s", "").toLowerCase match {
      case pattern(lower, upper, step) => lower.toInt to upper.toInt by step.toInt
      case _                           => throw new SparkException("Cannot parse config range")
    }
  }
}
