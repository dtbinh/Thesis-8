package com.sap.fugu.simulation

import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.BatchInfo
import org.apache.spark.streaming.scheduler.listener.model._
import org.scalatest.FunSuite

import scala.math.abs

class BatchSimulatorSuite extends FunSuite {
  import BatchSimulator._

  val DummyTask  = new TaskInfo(0, 0, 0, 0, "", "", null, false)
  val DummyStage = new StageInfo(0, 0, "", 0, Nil, Nil, "")
  val DummyJob   = SparkListenerJobStart(0, 0, Seq(DummyStage))
  val Batch      = BatchInfo(Time(MaxCacheSize), Map(), 0, Some(0), Some(1000), Map())

  def batch = BatchTier(Batch, Seq(job1, job2))

  def job1: JobTier = new JobTier(DummyJob, 100, MaxCacheSize, Seq(stage1, stage2, stage3)) {
    override val id: Long        = 1
    override val startTime: Long = 0
  }

  def stage1: StageTier = new StageTier(DummyStage, 1, Seq(task1, task2, task3)) {
    override val id: Long                  = 1
    override val parentStageIds: Seq[Long] = Nil
    override val startTime: Long           = 1
    override val stopTime: Long            = 25
  }

  def task1: TaskTier = new TaskTier(DummyTask, 1) {
    override val id: Long        = 1
    override val startTime: Long = 1
    override val stopTime: Long  = 10
  }

  def task2: TaskTier = new TaskTier(DummyTask, 1) {
    override val id: Long        = 2
    override val startTime: Long = 0
    override val stopTime: Long  = 10
  }

  def task3: TaskTier = new TaskTier(DummyTask, 1) {
    override val id: Long        = 3
    override val startTime: Long = 10
    override val stopTime: Long  = 20
  }

  def stage2: StageTier = new StageTier(DummyStage, 1, Seq(task4, task5)) {
    override val id: Long                  = 2
    override val parentStageIds: Seq[Long] = Nil
    override val startTime: Long           = 1
    override val stopTime: Long            = 50
  }

  def task4: TaskTier = new TaskTier(DummyTask, 2) {
    override val id: Long        = 4
    override val startTime: Long = 10
    override val stopTime: Long  = 30
  }

  def task5: TaskTier = new TaskTier(DummyTask, 2) {
    override val id: Long        = 5
    override val startTime: Long = 20
    override val stopTime: Long  = 50
  }

  def stage3: StageTier = new StageTier(DummyStage, 1, Seq(task6)) {
    override val id: Long                  = 3
    override val parentStageIds: Seq[Long] = Seq(1, 2)
    override val startTime: Long           = 55
    override val stopTime: Long            = 104
  }

  def task6: TaskTier = new TaskTier(DummyTask, 3) {
    override val id: Long        = 6
    override val startTime: Long = 60
    override val stopTime: Long  = 99
  }

  def job2: JobTier = new JobTier(DummyJob, 999, MaxCacheSize, Seq(stage4)) {
    override val id: Long        = 2
    override val startTime: Long = 105
  }

  def stage4: StageTier = new StageTier(DummyStage, 2, Seq(task7)) {
    override val id: Long                  = 4
    override val parentStageIds: Seq[Long] = Nil
    override val startTime: Long           = 110
    override val stopTime: Long            = 990
  }

  def task7: TaskTier = new TaskTier(DummyTask, 4) {
    override val id: Long        = 7
    override val startTime: Long = 110
    override val stopTime: Long  = 980
  }

  test("Simulate sample batch") {
    val simulator = new BatchSimulator(batch, 2)
    assert(abs(simulator.simulate().toDouble / batch.duration - 1.0) < 0.01)
  }

  test("Calculate time between jobs") {
    val time = calculateTimeBetweenJobs(batch)
    assert(time == 0 + 5 + 1)
  }

  test("Calculate time between stages") {
    val time = calculateTimeBetweenStages(job2)
    assert(time == 5 + 19)
  }

  test("Refresh cache if overflowed") {
    assert(!adjustCacheSize())

    (0 until MaxCacheSize) foreach { i =>
      calculateTimeBetweenJobs(new BatchTier(Batch, Nil) {
        override val delay: Long     = 0
        override val id: Long        = i
        override val records: Long   = 0
        override val startTime: Long = 0
        override val stopTime: Long  = 0
      })
    }

    val time1 = calculateTimeBetweenJobs(batch)

    assert(adjustCacheSize())
    assert(!adjustCacheSize())

    val time2 = calculateTimeBetweenJobs(new BatchTier(Batch, Nil) {
      override val delay: Long     = 0
      override val id: Long        = MaxCacheSize
      override val records: Long   = 0
      override val startTime: Long = 0
      override val stopTime: Long  = 0
    })

    assert(time1 == time2)
  }

}
