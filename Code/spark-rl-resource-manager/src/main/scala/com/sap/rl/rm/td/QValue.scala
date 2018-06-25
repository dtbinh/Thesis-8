package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action

case class QValue(action: Action, expectedReward: Double)
