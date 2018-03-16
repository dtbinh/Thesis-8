package org.apache.spark.streaming.scheduler.listener

import java.util.Properties

import org.apache.spark.SparkContext.RDD_SCOPE_KEY
import org.apache.spark.SparkException
import org.apache.spark.Success
import org.apache.spark.scheduler._
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.BatchInfo
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.scalatest.FunSuite

class BatchListenerSuite extends FunSuite {
  val taskInfo = new TaskInfo(0, 0, 0, 0, "", "", TaskLocality.ANY, false) { finishTime = 1 }
  val taskEnd  = SparkListenerTaskEnd(0, 0, "", Success, taskInfo, null)

  val stageInfo = new StageInfo(0, 0, "", 0, Nil, Nil, "") {
    submissionTime = Some(0)
    completionTime = Some(1)
  }
  val stageSubmitted = SparkListenerStageSubmitted(stageInfo)
  val stageCompleted = SparkListenerStageCompleted(stageInfo)

  val jobStart = SparkListenerJobStart(0, 0, Seq(stageInfo), createJobProperty(0))
  val jobEnd   = SparkListenerJobEnd(0, 0, JobSucceeded)

  val batchInfo      = BatchInfo(Time(0), Map(), 0, Some(1), Some(100), Map())
  val batchCompleted = StreamingListenerBatchCompleted(batchInfo)

  test("Add valid job") {
    val listener = new BatchListener() {}

    listener.onJobStart(jobStart)
    listener.onJobEnd(jobEnd)
    listener.onBatchCompleted(batchCompleted)

    println(listener.batches.map(_.id))

    assert(listener.batches.flatMap(_.jobs).nonEmpty)
  }

  test("Fail on adding job to unknown batch") {
    val listener = new BatchListener() {}
    listener.onBatchCompleted(batchCompleted)

    assert(listener.batches.flatMap(_.jobs).isEmpty)
    listener.onJobStart(SparkListenerJobStart(0, 0, Nil, createJobProperty(5)))
    assert(listener.batches.flatMap(_.jobs).isEmpty)
  }

  test("Fail on adding invalid job") {
    val listener = new BatchListener() {}
    assert(listener.batches.isEmpty)
    listener.onJobStart(SparkListenerJobStart(0, 0, Nil, createJobProperty(1)))
    assert(listener.batches.isEmpty)
  }

  test("Fail on adding job with invalid properties") {
    val listener = new BatchListener() {}

    assertThrows[SparkException] {
      val properties = new Properties()

      properties.setProperty(RDD_SCOPE_KEY, "\"id\":\"FOO\"")
      listener.onJobStart(SparkListenerJobStart(0, 0, Nil, properties))
      listener.onJobEnd(SparkListenerJobEnd(0, 0, JobSucceeded))
    }
  }

  test("Add valid stage") {
    val listener = new BatchListener() {}

    listener.onJobStart(jobStart)
    listener.onStageCompleted(stageCompleted)
    listener.onJobEnd(jobEnd)
    listener.onBatchCompleted(batchCompleted)

    assert(listener.batches.flatMap(_.jobs.flatMap(_.stages)).nonEmpty)
  }

  test("Fail on adding invalid stage") {
    val listener = new BatchListener() {}

    listener.onBatchCompleted(batchCompleted)
    assert(listener.batches.flatMap(_.jobs.flatMap(_.stages)).isEmpty)
    listener.onStageSubmitted(stageSubmitted)
    assert(listener.batches.flatMap(_.jobs.flatMap(_.stages)).isEmpty)
  }

  test("Add successful, valid task") {
    val listener = new BatchListener() {}

    listener.onJobStart(jobStart)
    listener.onTaskEnd(taskEnd)
    listener.onStageCompleted(stageCompleted)
    listener.onJobEnd(jobEnd)
    listener.onBatchCompleted(batchCompleted)

    assert(listener.batches.flatMap(_.jobs.flatMap(_.stages.flatMap(_.tasks))).nonEmpty)
  }

  test("Do nothing on unsuccessful task") {
    val listener = new BatchListener() {}
    val taskInfo = new TaskInfo(0, 0, 0, 0, "", "", null, false)

    listener.onTaskEnd(SparkListenerTaskEnd(0, 0, "", Success, taskInfo, null))
  }

  test("Fail on adding invalid task") {
    val listener = new BatchListener() {}

    assert(listener.batches.flatMap(_.jobs.flatMap(_.stages.flatMap(_.tasks))).isEmpty)
    listener.onTaskEnd(taskEnd)
    assert(listener.batches.flatMap(_.jobs.flatMap(_.stages.flatMap(_.tasks))).isEmpty)
  }

  test("Add valid, finished stage") {
    val listener = new BatchListener() {}

    listener.onJobStart(jobStart)
    listener.onStageSubmitted(stageSubmitted)
    listener.onStageCompleted(stageCompleted)
    listener.onJobEnd(jobEnd)
    listener.onBatchCompleted(batchCompleted)

    assert(listener.batches.flatMap(_.jobs.flatMap(_.stages)).nonEmpty)
    listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))
    assert(listener.batches.flatMap(_.jobs.flatMap(_.stages)).nonEmpty)
  }

  test("Remove valid, finished stage if unsuccessful") {
    val listener = new BatchListener() {}

    val info = new StageInfo(0, 0, "", 0, Nil, Nil, "")

    listener.onJobStart(jobStart)
    listener.onStageCompleted(SparkListenerStageCompleted(info))
    listener.onJobEnd(jobEnd)
    listener.onBatchCompleted(batchCompleted)

    assert(listener.batches.flatMap(_.jobs.flatMap(_.stages)).isEmpty)
  }

  test("Add job completion time for valid, succeeded job") {
    val listener = new BatchListener() {}

    listener.onJobStart(jobStart)
    listener.onJobEnd(SparkListenerJobEnd(0, 0, JobSucceeded))
    listener.onBatchCompleted(batchCompleted)

    assert(listener.batches.head.jobs.nonEmpty)
  }

  test("Remove valid job if failed") {
    val listener = new BatchListener() {}

    listener.onJobStart(jobStart)
    listener.onJobEnd(SparkListenerJobEnd(0, 0, JobFailed(null)))
    listener.onBatchCompleted(batchCompleted)

    assert(listener.batches.flatMap(_.jobs).isEmpty)
  }

  test("Update batch processing time upon completion") {
    val listener = new BatchListener() {}
    val endTime  = 200
    val info     = BatchInfo(Time(0), Map(), 0, Some(1), Some(endTime), Map())

    listener.onBatchCompleted(StreamingListenerBatchCompleted(info))
    assert(listener.batches.headOption.map(_.stopTime).getOrElse(0L) == endTime)
  }

  private def createJobProperty(batchTime: Long): Properties = {
    val properties = new Properties()
    properties.setProperty(RDD_SCOPE_KEY, "\"id\":\"1_" + batchTime + "\"")
    properties
  }
}
