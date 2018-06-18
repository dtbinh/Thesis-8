package com.sap.rl.window

import java.util.concurrent.LinkedBlockingQueue

class UnboundedWindow {

  private val window: LinkedBlockingQueue[Int] = new LinkedBlockingQueue[Int]()

  @throws(classOf[IllegalArgumentException])
  def add(element: Int): Unit = {
    if (element < 0) {
      throw new IllegalArgumentException(s"${element} is negative")
    }
    window.put(element)
  }

  def size(): Int = window.size()

  def isEmpty: Boolean = window.size() == 0

  def averageOfFirstElements(numberOfElements: Int): Double = {
    val windowSize: Int = window.size()
    val numberOfElementsToPop = if (numberOfElements <= windowSize) numberOfElements else windowSize

    if (numberOfElementsToPop == 0) return 0

    var sum: Int = 0
    (1 to numberOfElementsToPop) foreach { _ =>
      sum = sum + window.poll()
    }

    sum.toDouble / numberOfElementsToPop
  }
}