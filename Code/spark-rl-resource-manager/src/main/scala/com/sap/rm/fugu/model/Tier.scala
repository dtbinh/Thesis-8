package com.sap.rm.fugu.model

/**
 * Trait fixing functionality of each tier model. Not only does it specify mandatory fields but also
 * introduces F-bounded polymorphism with Ordered trait allowing easy sorting of child objects.
 */
trait Tier[T <: Tier[T]] extends Ordered[T] {
  def id: Long
  def startTime: Long
  def stopTime: Long
  def toLogString: String

  def duration: Long = stopTime - startTime

  override def <(that: T): Boolean   = compare(that, _ < _)
  override def >(that: T): Boolean   = compare(that, _ > _)
  override def <=(that: T): Boolean  = compare(that, _ <= _)
  override def >=(that: T): Boolean  = compare(that, _ >= _)
  override def compare(that: T): Int = compare(that, _ compare _)

  override def toString: String =
    s"""TIER:
       |  id:              $id
       |  processing time: $startTime to $stopTime ($duration)
     """.stripMargin

  protected def toLogString(lowerTiers: Seq[Tier[_]]): String =
    s"$toString${lowerTiers.map(_.toLogString).mkString}"

  private def compare[U](that: T, compare: (Long, Long) => U): U = {
    if (this.startTime != that.startTime) {
      compare(this.startTime, that.startTime)
    } else {
      compare(this.stopTime, that.stopTime)
    }
  }
}
