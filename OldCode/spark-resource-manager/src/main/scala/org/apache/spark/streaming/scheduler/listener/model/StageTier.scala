package org.apache.spark.streaming.scheduler.listener.model

import java.lang.System.currentTimeMillis

import org.apache.spark.scheduler.StageInfo

case class StageTier(info: StageInfo, jobId: Long, tasks: Seq[TaskTier]) extends Tier[StageTier] {
  val parentStageIds: Seq[Long] = info.parentIds.map(_.toLong)

  override val id: Long        = info.stageId
  override val startTime: Long = info.submissionTime.getOrElse(currentTimeMillis())
  override val stopTime: Long  = info.completionTime.getOrElse(startTime)

  override def toLogString: String = toLogString(tasks)
  override def toString: String =
    s"""  STAGE:
       |    id:                 $id ($id.${info.attemptId})
       |    name:               ${info.name}
       |    processing time:    $startTime to $stopTime ($duration)
       |    parents:            ${info.parentIds.mkString(", ")}
       |    tasks:              ${tasks.map(_.id).mkString(", ")}
       |    status:             ${info.getStatusString}\n""".stripMargin
}
