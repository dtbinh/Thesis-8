package com.sap.rm.rl

object Action extends Enumeration {
  type Action = Value

  val ScaleIn, ScaleOut, NoAction = Value
}
