package com.sap.rl.rm.td

import com.sap.rl.rm.commons.Action
import org.apache.spark.SparkConf
import org.apache.spark.streaming.scheduler.RMConstants
import org.scalatest.{BeforeAndAfter, FunSuite}

class TDStateSpaceTest extends FunSuite with BeforeAndAfter {

  import org.apache.spark.streaming.scheduler.RMConstants._
  import com.sap.rl.TestCommons._

  var sparkConf: SparkConf = _

  before {
    sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
      .set(DynamicResourceAllocationEnabledKey, "true")
      .set(DynamicResourceAllocationTestingKey, "true")
      .set(CoresPerTaskKey, "1")
      .set(CoresPerExecutorKey, "1")
      .set(MinimumExecutorsKey, "5")
      .set(MaximumExecutorsKey, "25")
      .set(LatencyGranularityKey, "20")
      .set(MinimumLatencyKey, "300")
      .set(TargetLatencyKey, "800")
      .set(MaximumLatencyKey, "10000")
  }

  test("testInitialization") {
    val constants: RMConstants = RMConstants(sparkConf)
    import constants._

    val stateSpace = TDStateSpace(constants)

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) * (MaximumLatency / LatencyGranularity)
    assert(stateSpace.value.size == expectedSpaceSize)
  }

  test("bestActionFor") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = TDStateSpace(constants)

    assert(QValue(Action.ScaleOut, 1) == stateSpace.value(State(12, 150))(0))
    assert(QValue(Action.ScaleIn, 1) == stateSpace.value(State(12, 1))(2))
  }
}
