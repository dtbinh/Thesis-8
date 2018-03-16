package com.sap.fugu.optimizer

import org.scalatest.FunSuite

import scala.math.sin

class AdaptiveWindowSuite extends FunSuite {

  test("Simple function") {
    val adwin = new AdaptiveWindow(0.1, 32)
    (1 to 1024).foreach(n => adwin.add(f(n)))

    assert(adwin.getWidth == 384)
  }

  private def f(n: Int) = n * (sin(n) + 1)
}
