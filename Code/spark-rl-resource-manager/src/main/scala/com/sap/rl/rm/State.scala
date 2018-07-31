package com.sap.rl.rm

case class State(numberOfExecutors: Int, latency: Int) extends Ordered[State] {

  override def compare(that: State): Int = {

    import ResourceManagerConfig._

    if (numberOfExecutors < that.numberOfExecutors) LessThan
    else if (numberOfExecutors > that.numberOfExecutors) GreaterThan
    else if (latency < that.latency) LessThan
    else if (latency > that.latency) GreaterThan
    else Equal
  }
}
