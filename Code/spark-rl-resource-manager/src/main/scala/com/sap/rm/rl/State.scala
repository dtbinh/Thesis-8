package com.sap.rm.rl

import com.sap.rm.ResourceManagerConfig._

@SerialVersionUID(1L)
case class State(latency: Int, loadIsIncreasing: Boolean) extends Serializable with Ordered[State] {

  override def compare(that: State): Int = {
    if (latency < that.latency) LessThan
    else if (latency > that.latency) GreaterThan
    else if (!loadIsIncreasing) LessThan
    else if (loadIsIncreasing) GreaterThan
    else Equal
  }

  override def toString: String = "State(Latency=%d,loadIsIncreasing=%b)".format(latency, loadIsIncreasing)
}
