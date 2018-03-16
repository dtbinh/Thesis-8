package org.apache.spark.streaming.scheduler.listener

import org.apache.spark.SparkContext.{RDD_SCOPE_KEY => RddScopeKey}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.spark.streaming.scheduler.listener.model.BatchTier
import org.apache.spark.streaming.scheduler.listener.model.JobTier
import org.apache.spark.streaming.scheduler.listener.model.StageTier
import org.apache.spark.streaming.scheduler.listener.model.TaskTier

import scala.collection.mutable

/**
 * Listener for batch-related events. It registers all entities involved in batch evaluation.
 *
 * By default each batch consists of three tiers:
 *   - Job: Batch subquery matching single output
 *   - Stage: Job subquery up to first wide/stateful operator.
 *   - Task: Stage query for a single dataset partition
 *
 * All information is gathered as partial results in mutable fields of this listener and upon
 * registering entity completion an immutable instance is produced. In the end all internal
 * references form a tree of objects exactly matching query processing steps.
 *
 * Since trait with parameters won't be supported until Dotty compiler release this class does
 * incorporate bufferSize field setting upper bound for batch history. If not set the system won't
 * delete old batches which might lead to heap overflow.
 */
trait BatchListener extends SparkListenerT with StreamingListener with Logging {
  import BatchListener._

  var bufferSize: Option[Int] = None

  private val batchQueue = new mutable.Queue[BatchTier]()

  private val batchIdToJobs  = new mutable.HashMap[Long, List[JobTier]]()
  private val jobIdToStages  = new mutable.HashMap[Long, List[StageTier]]()
  private val stageIdToTasks = new mutable.HashMap[Long, List[TaskTier]]()

  // Help with finding relations between entities. It's a problem related to original models.
  private val stageIdToJobId  = new mutable.HashMap[Long, Long]()
  private val jobIdToJobStart = new mutable.HashMap[Long, SparkListenerJobStart]()

  // We don't want to pass a reference to mutating object.
  def batches: Seq[BatchTier] = batchQueue.clone()

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

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit =
    synchronized {
      val info      = batchCompleted.batchInfo
      val batchTime = info.batchTime.milliseconds
      val batch     = BatchTier(info, pollJobs(batchTime))
      log.info(s"Received successful batch $batchTime ms with completion time")
      log.debug(batch.toLogString)

      if (bufferSize.contains(batchQueue.size)) batchQueue.dequeue()
      batchQueue.enqueue(batch)
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
}

object BatchListener {
  // This is the only certain way to extract dependency between job and batch
  def batchTime(jobStart: SparkListenerJobStart): Long = {
    val pattern       = ".*\"id\":\"[0-9]+_([0-9]+)\".*".r
    val scopeProperty = jobStart.properties.getProperty(RddScopeKey)

    scopeProperty.replaceAll("\\s", "") match {
      case pattern(batchTime) => batchTime.toLong
      case _                  => throw new SparkException(s"Could not extract batch time")
    }
  }
}
