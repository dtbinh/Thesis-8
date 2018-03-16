package org.apache.spark.streaming.scheduler.listener.model

import org.apache.spark.scheduler.{SparkListenerJobStart => JobStart}

case class JobTier(jobStart: JobStart, stopTime: Long, batchId: Long, stages: Seq[StageTier])
    extends Tier[JobTier] {
  val stageIds: Seq[Long]       = jobStart.stageInfos.map(_.stageId.toLong)
  val absentStageIds: Seq[Long] = stageIds.diff(stages.map(_.id))

  override val id: Long        = jobStart.jobId
  override val startTime: Long = jobStart.time

  override def toLogString: String = toLogString(stages)
  override def toString: String =
    s""" JOB:
       |   id:                $id
       |   processing time:   $startTime to $stopTime ($duration)
       |   stages:            ${stageIds.mkString(", ")} (${stages.map(_.id).mkString(", ")})
       |   absent stages:     ${absentStageIds.mkString(", ")}\n""".stripMargin
}
