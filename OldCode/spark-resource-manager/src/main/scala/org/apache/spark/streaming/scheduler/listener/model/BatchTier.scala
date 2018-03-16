package org.apache.spark.streaming.scheduler.listener.model

import org.apache.spark.streaming.scheduler.BatchInfo

case class BatchTier(info: BatchInfo, jobs: Seq[JobTier]) extends Tier[BatchTier] {
  val delay: Long   = info.processingStartTime.map(_ - info.submissionTime).getOrElse(0L)
  val records: Long = info.numRecords

  override val id: Long        = info.batchTime.milliseconds
  override val startTime: Long = info.processingStartTime.getOrElse(info.submissionTime)
  override val stopTime: Long  = info.processingEndTime.getOrElse(startTime)

  override def toLogString: String = toLogString(jobs)
  override def toString: String =
    s"""BATCH:
       |  id (batch time):   $id [ms]
       |  submission time:   ${info.submissionTime} ($delay)
       |  processing time:   $startTime to $stopTime ($duration)
       |  records:           $records
       |  jobs:              ${jobs.map(_.id).mkString(", ")}\n""".stripMargin
}
