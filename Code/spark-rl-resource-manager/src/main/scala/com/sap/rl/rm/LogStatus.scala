package com.sap.rl.rm

object LogStatus extends Enumeration {
  type Status = Value

  val APP_STARTED: Status = Value
  val APP_ENDED: Status = Value
  val STREAMING_STARTED: Status = Value

  val SPARK_EXEC_ADDED: Status = Value
  val SPARK_EXEC_REMOVED: Status = Value

  val SLO_VIOLATION: Status = Value

  val BATCH_EMPTY: Status = Value
  val BATCH_OK: Status = Value
  val START_UP: Status = Value
  val GRACE_PERIOD: Status = Value

  val FIRST_WINDOW: Status = Value
  val WINDOW_FULL: Status = Value
  val WINDOW_ADDED: Status = Value

  val DECIDED: Status = Value

  val EXEC_KILL_OK: Status = Value
  val EXEC_ADD_OK: Status = Value
  val EXEC_ADD_ERR: Status = Value
  val EXEC_EXCESSIVE: Status = Value
  val EXEC_NOT_ENOUGH: Status = Value
  val EXEC_NO_ACTION: Status = Value

  val STATE_OK: Status = Value
  val INVALID_STATE_EXCESSIVE_EXECUTORS: Status = Value
  val INVALID_STATE_EXCESSIVE_LATENCY: Status = Value
  val INVALID_STATE_EXCESSIVE_INCOMING_MESSAGES: Status = Value
}
