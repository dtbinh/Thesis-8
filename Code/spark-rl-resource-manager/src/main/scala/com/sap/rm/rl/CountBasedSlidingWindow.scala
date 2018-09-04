package com.sap.rm.rl

import scala.collection.mutable.{Queue => MutableQueue}

class CountBasedSlidingWindow(length: Int) {

  private val window: MutableQueue[Int] = MutableQueue[Int]()
  private var sum: Int = 0

  def add(element: Int): Unit = {
    window += element
    sum += element
    if (window.size > length) sum -= window.dequeue()
  }

  def average(): Double = sum.toDouble / window.size

  override def toString: String = "CountBasedSlidingWindow(Sum=%d,Count=%d)".format(sum, window.size)
}

object CountBasedSlidingWindow {
  def apply(length: Int): CountBasedSlidingWindow = new CountBasedSlidingWindow(length)
}
