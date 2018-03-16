package com.sap.fugu.simulation

import org.apache.spark.streaming.scheduler.listener.model.Tier

/**
 * Since Scala implementation of Ordered trait doesn't allow for sorting mixed subclasses it's
 * needed to instantiate sequence under single class.
 *
 * @param startTime Start time of the tier
 * @param stopTime  Stop time of the tier
 */
case class EmptyTier(startTime: Long, stopTime: Long) extends Tier[EmptyTier] {
  def this(tier: Tier[_]) = this(tier.startTime, tier.stopTime)

  override def id: Long            = 0
  override def toLogString: String = toString
}
