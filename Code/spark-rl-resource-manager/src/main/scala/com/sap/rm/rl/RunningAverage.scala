package com.sap.rm.rl

@SerialVersionUID(1L)
class RunningAverage(initialValue: Int = 0, initialCount: Int = 0) extends Serializable {

  private var _sum: Int = initialValue
  private var _count: Int = initialCount

  def add(value: Int): Unit = {
    _sum += value
    _count += 1
  }

  def average(): Double = _sum.toDouble / _count

  def reset(): Unit = {
    _sum = 0
    _count = 0
  }

  def count(): Int = _count


  override def toString: String = "RunningAverage(Sum=%d,Count=%d)".format(_sum, _count)
}

object RunningAverage {
  def apply(initialValue: Int = 0, initialCount: Int = 0): RunningAverage = new RunningAverage(initialValue, initialCount)
}