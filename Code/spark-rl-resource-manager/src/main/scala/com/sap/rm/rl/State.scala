package com.sap.rm.rl

import com.sap.rm.ResourceManagerConfig._

case class State(numberOfExecutors: Int, latency: Int) extends Ordered[State] {

  override def toString: String = "State(NumberOfExecutors=%d,Latency=%d)".format(numberOfExecutors, latency)

  override def compare(that: State): Int = {
    if (numberOfExecutors < that.numberOfExecutors) LessThan
    else if (numberOfExecutors > that.numberOfExecutors) GreaterThan
    else if (latency < that.latency) LessThan
    else if (latency > that.latency) GreaterThan
    else Equal
  }
}
