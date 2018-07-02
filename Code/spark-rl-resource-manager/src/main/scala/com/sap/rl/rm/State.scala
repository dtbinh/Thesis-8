package com.sap.rl.rm

case class State(numberOfExecutors: Int, latency: Int) extends Ordered[State] {
  override def compare(that: State): Int = {
    if (numberOfExecutors < that.numberOfExecutors) -1
    else if (numberOfExecutors > that.numberOfExecutors) 1
    else if (latency < that.latency) -1
    else if (latency > that.latency) 1
    else 0
  }
}
