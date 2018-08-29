package com.sap.rm

object LogTags extends Enumeration {
  type Status = Value

  val APP_ENDED: Status = Value
  val STREAMING_STARTED: Status = Value

  val SPARK_EXEC_ADDED: Status = Value
  val SPARK_EXEC_REMOVED: Status = Value

  val STAT: Status = Value

  val BATCH_EMPTY: Status = Value
  val BATCH_OK: Status = Value
  val BATCH_STARTUP: Status = Value
  val GRACE_PERIOD: Status = Value

  val FIRST_WINDOW: Status = Value
  val WINDOW_FULL: Status = Value
  val WINDOW_ADDED: Status = Value

  val DECIDED: Status = Value

  val EXEC_KILL_OK: Status = Value
  val EXEC_ADD_OK: Status = Value
  val EXEC_ADD_ERR: Status = Value

  val REMOVED_ACTION_SCALE_OUT: Status = Value
  val REMOVED_ACTION_SCALE_IN: Status = Value

  val EXCESSIVE_LATENCY: Status = Value

  val RANDOM_ACTION: Status = Value
  val OPTIMAL_ACTION: Status = Value
}
