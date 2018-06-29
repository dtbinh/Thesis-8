package com.sap.rl.rm

object LogStatus extends Enumeration {
  type Status = Value

  val BATCH_EMPTY = Value
  val BATCH_OK = Value

  val START_UP = Value
  val NOT_START_UP = Value

  val GRACE_PERIOD = Value
  val NOT_GRACE_PERIOD = Value

  val FIRST_WINDOW = Value
  val WINDOW_FULL = Value
  val NOT_WINDOW_FULL = Value

  val DECIDED = Value

  val EXEC_KILL_OK = Value
  val EXEC_KILL_NOT_ENOUGH = Value

  val EXEC_ADD_OK = Value
  val EXEC_ADD_ERR = Value
  val EXEC_ADD_EXCESSIVE = Value

  val EXEC_NO_ACTION = Value
}
