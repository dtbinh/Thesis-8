package com.sap.fugu.simulation

import org.scalatest.FunSuite

class EmptyTierSuite extends FunSuite {

  test("Valid duration and completion flag") {
    val tier = EmptyTier(0, 1)
    assert(tier.duration == 1)
  }

  test("Sort tiers") {
    val sortedTiers = Seq(
      EmptyTier(0, 0),
      EmptyTier(1, 1),
      EmptyTier(-1, -1),
      EmptyTier(-1, 0),
      EmptyTier(-1, -2),
      EmptyTier(0, 0),
      EmptyTier(0, 100),
      EmptyTier(100, 0),
      EmptyTier(1, -100),
      EmptyTier(1, 100),
      EmptyTier(1, -2),
      EmptyTier(-100, 100)
    ).sorted

    sortedTiers.sliding(2).toSeq foreach {
      case Seq(a, b) =>
        assert(a <= b)
        assert(!(a > b))
        assert(a == b || (a < b && !(a >= b)))
    }

    val startTimes = sortedTiers.map(_.startTime)
    startTimes.sliding(2) foreach {
      case Seq(a, b) => assert(a <= b)
    }

    val stopTimes = sortedTiers.groupBy(_.startTime).values.map(_.map(_.stopTime)).toSeq
    stopTimes.foreach(_.sliding(2).toList foreach {
      case List(a, b) => assert(a <= b)
      case List(_)    =>
    })
  }

}
