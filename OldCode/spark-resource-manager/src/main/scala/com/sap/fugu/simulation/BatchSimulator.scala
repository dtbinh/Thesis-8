package com.sap.fugu.simulation

import org.apache.spark.SparkException
import org.apache.spark.streaming.scheduler.listener.model._

import scala.collection.mutable
import scala.math.max

/**
 * FIFO simulator for batch model. It delimits tasks by jobs and parallelizes them by stages. All of
 * the computation is mostly done on heap structure making add/remove operations fast enough.
 *
 * The assumption here is that tasks are scheduled in order of arrival whenever processing of
 * parent stages is completed. If a task is finished then executor is released and next task from
 * the pool is scheduled. If there are no tasks left in the pool then stage is marked as completed.
 * When there are no stages left then job is marked as completed and next ones are processed in a
 * sequential order.
 *
 * The simulator also takes into consideration static time of the cluster, i.e. time that was not
 * used for performing any computations (usually it's sacrificed for I/O or network operations).
 * Those partial results are cachable and stored in companion object. Since it's not a critical
 * section this cache is by default limited to 10,000 entries.
 *
 * @param batch      Batch to simulate
 * @param executorNo Number of executors to use
 */
class BatchSimulator(batch: BatchTier, executorNo: Int) {
  import BatchSimulator._

  private val executors = new mutable.PriorityQueue[ExecutorTime]()(Ordering.by(-_))

  def simulate(): Long = {
    adjustCacheSize()

    val time            = batch.jobs.map(simulate).sum
    val timeBetweenJobs = calculateTimeBetweenJobs(batch)

    time + timeBetweenJobs
  }

  private def simulate(job: JobTier): Long = {
    val stageIdToExecutorTime = new mutable.HashMap[Long, ExecutorTime]().withDefaultValue(0L)

    executors.clear()
    executors ++= Seq.fill(executorNo)(0L)

    job.stages foreach { stage =>
      val parentStagesEndTimes = stage.parentStageIds.map(stageIdToExecutorTime)
      val parentStageEndTime   = parentStagesEndTimes.reduceOption(_ max _).getOrElse(0L)
      stageIdToExecutorTime(stage.id) = simulate(stage, parentStageEndTime)
    }

    val time              = executors.max
    val timeBetweenStages = calculateTimeBetweenStages(job)

    time + timeBetweenStages
  }

  private def simulate(stage: StageTier, parentStageEndTime: ExecutorTime): Long = {
    val sortedTasks         = stage.tasks.sorted
    val taskCompletionTimes = sortedTasks.map(simulate(_, parentStageEndTime))

    taskCompletionTimes.reduceOption(_ max _).getOrElse(0L)
  }

  private def simulate(task: TaskTier, parentStageEndTime: ExecutorTime): Long = {
    val executorTime  = executors.dequeue()
    val taskStartTime = max(parentStageEndTime, executorTime)
    val taskEndTime   = taskStartTime + task.duration

    executors.enqueue(taskEndTime)
    taskEndTime
  }
}

object BatchSimulator {
  type ExecutorTime = Long

  final val MaxCacheSize: Int = 10000

  // To be replaced with TreeMap in Scala 2.12 for faster key deletion
  private val batchTimeToEmptyTime = new mutable.HashMap[Long, Long]()
  private val jobIdToEmptyTime     = new mutable.HashMap[Long, Long]()

  def adjustCacheSize(size: Int = MaxCacheSize): Boolean = {
    val isBatchCacheOverflowed = adjustCacheSize(batchTimeToEmptyTime, size)
    val isJobCacheOverflowed   = adjustCacheSize(jobIdToEmptyTime, size)
    isBatchCacheOverflowed || isJobCacheOverflowed
  }

  def calculateTimeBetweenJobs(batch: BatchTier): Long =
    calculateWithCache(batch)(() => {
      val timesBetween = createTierSeq(batch, batch.jobs).sliding(2) map {
        case Seq(prev, next) => max(next.startTime - prev.stopTime, 0)
      }

      timesBetween.sum
    })

  def calculateTimeBetweenStages(job: JobTier): Long =
    calculateWithCache(job)(() => {
      val taskBounds = job.stages map { stage =>
        val tasks         = stage.tasks
        val firstTaskTime = tasks.map(_.startTime).reduceOption(_ min _).getOrElse(stage.startTime)
        val lastTaskTime  = tasks.lastOption.map(_.stopTime).getOrElse(stage.stopTime)
        EmptyTier(firstTaskTime, lastTaskTime)
      }

      val timesBetween = createTierSeq(job, taskBounds).sorted.sliding(2) map {
        case Seq(prev, next) => max(next.startTime - prev.stopTime, 0)
      }

      timesBetween.sum
    })

  private def adjustCacheSize(map: mutable.Map[Long, Long], size: Int): Boolean = synchronized {
    val isOverflowed = map.size > size

    if (isOverflowed) map.keySet.toSeq.sorted.take(map.size - size).foreach(map -= _)
    isOverflowed
  }

  private def calculateWithCache(tier: Tier[_])(calculate: () => Long): Long = synchronized {
    tier match {
      case batch: BatchTier => calculateWithCache(batchTimeToEmptyTime, batch.id, calculate)
      case job: JobTier     => calculateWithCache(jobIdToEmptyTime, job.id, calculate)
      case _                => throw new SparkException("Can't find cache for given Tier")
    }
  }

  private def createTierSeq(tier: Tier[_], middle: Seq[Tier[_]]): Seq[EmptyTier] = {
    val first = EmptyTier(tier.startTime, tier.startTime)
    val last  = EmptyTier(tier.stopTime, tier.stopTime)
    Seq(first) ++ middle.map(new EmptyTier(_)) ++ Seq(last)
  }

  private def calculateWithCache(map: mutable.Map[Long, Long], key: Long, calculate: () => Long) = {
    map.get(key) match {
      case Some(time) => time
      case None =>
        val value = calculate()
        map(key) = value
        value
    }
  }
}
