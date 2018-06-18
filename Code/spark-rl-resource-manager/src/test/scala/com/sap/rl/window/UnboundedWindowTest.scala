package com.sap.rl.window

import org.scalatest.FunSuite

class UnboundedWindowTest extends FunSuite {

  test("AverageOfZeroElementsIsZero") {
    val window = new UnboundedWindow

    assert(window.averageOfFirstElements(10) == 0)
    assert(window.size() == 0)
    assert(window.isEmpty)
  }

  test("AverageOfFiveElements") {
    val window = new UnboundedWindow
    window.add(12)
    window.add(10)
    window.add(110)
    window.add(0)
    window.add(43)

    assert(window.averageOfFirstElements(5) == 35)
    assert(window.size() == 0)
    assert(window.isEmpty)
  }

  test("AverageOfTwoElementsOutOfFiveElements") {
    val window = new UnboundedWindow
    window.add(12)
    window.add(10)
    window.add(110)
    window.add(0)
    window.add(43)

    assert(window.averageOfFirstElements(2) == 11)
    assert(window.size() == 3)
    assert(!window.isEmpty)
  }

  test("AverageOfFiveElementsOutOfTwoElements") {
    val window = new UnboundedWindow
    window.add(110)
    window.add(0)

    assert(window.averageOfFirstElements(5) == 55)
    assert(window.size() == 0)
    assert(window.isEmpty)
  }

  test("AppendTwoElements") {
    val window = new UnboundedWindow
    assert(window.size() == 0)

    window.add(12)
    window.add(11)

    assert(window.size() == 2)
    assert(!window.isEmpty)
  }

  test("AppendNegativeElementShouldFail") {
    val window = new UnboundedWindow
    assert(window.size() == 0)

    window.add(12)
    assertThrows[IllegalArgumentException] {
      window.add(-1)
    }
  }
}
