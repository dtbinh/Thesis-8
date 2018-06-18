package com.sap.rl.window

import java.util.concurrent.LinkedBlockingQueue

/**
  * Thread safe implementation of Window.
  * There is no limit on size of the window.
  * Elements will be removed after taking average
  */
class UnboundedBlockingWindow extends Window {

  private val window: LinkedBlockingQueue[Long] = new LinkedBlockingQueue[Long]()

  @throws(classOf[IllegalArgumentException])
  override def add(element: Long): Unit = {
    if (element < 0) {
      throw new IllegalArgumentException(s"${element} is negative")
    }
    window.put(element)
  }

  override def size(): Int = window.size()

  override def averageOfFirstElements(numberOfElements: Int): Double = {
    val windowSize: Int = window.size()
    val numberOfElementsToPop = if (numberOfElements <= windowSize) numberOfElements else windowSize

    if (numberOfElementsToPop == 0) return 0

    var sum: Long = 0
    (1 to numberOfElementsToPop) foreach { _ =>
      sum = sum + window.poll()
    }

    sum.toDouble / numberOfElementsToPop
  }
}