package com.sap.rm.rl

import org.scalatest.FunSuite

class CountBasedSlidingWindowTest extends FunSuite {

  test("testAdd") {
    val window = CountBasedSlidingWindow(5)

    window.add(1)
    window.add(2)
    window.add(3)
    assert(2.0 == window.average())

    window.add(4)
    window.add(5)
    assert(3.0 == window.average())

    window.add(6)
    assert(4.0 == window.average())

    window.add(7)
    assert(5.0 == window.average())

    window.add(8)
    assert(6.0 == window.average())
  }

}
