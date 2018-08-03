package com.sap.rm.fugu.model

import org.apache.spark.scheduler.TaskInfo

case class TaskTier(info: TaskInfo, stageId: Long) extends Tier[TaskTier] {
  override val id: Long        = info.taskId
  override val startTime: Long = info.launchTime
  override val stopTime: Long  = if (info.failed) startTime else info.finishTime

  override def toLogString: String = toString
  override def toString: String =
    s"""   TASK:
       |     id:              $id (${info.id})
       |     processing time: $startTime to $stopTime ($duration)
       |     executor id:     ${info.executorId}
       |     locality:        ${info.taskLocality.toString}
       |     status:          ${info.status}\n""".stripMargin
}
