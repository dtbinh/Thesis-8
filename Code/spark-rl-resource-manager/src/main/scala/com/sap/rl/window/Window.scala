package com.sap.rl.window

trait Window {

  @throws(classOf[IllegalArgumentException])
  def add(element: Long): Unit

  def size(): Int

  def isEmpty: Boolean = size() == 0

  def averageOfFirstElements(numberOfElements: Int): Double
}
