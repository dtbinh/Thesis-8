package com.sap.rl

import com.sap.rl.util.Precision

object implicits {
  implicit class DoubleWithAlmostEquals(val lhs: Double) extends AnyVal {
    def ~=(rhs: Double)(implicit precision: Precision) = (lhs - rhs).abs <= precision.value
  }
}
