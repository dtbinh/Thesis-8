package com.sap.util

object implicits {
  implicit class DoubleWithAlmostEquals(val lhs: Double) extends AnyVal {
    def ~=(rhs: Double)(implicit precision: Precision): Boolean = (lhs - rhs).abs <= precision.value
  }
}
