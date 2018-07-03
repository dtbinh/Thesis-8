package com.sap.rl

import com.sap.rl.rm.Action._
import com.sap.rl.rm.{State, StateSpace}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.scheduler.RMConstants
import org.scalatest.{BeforeAndAfter, FunSuite}

class StateSpaceTest extends FunSuite with BeforeAndAfter {

  import com.sap.rl.TestCommons._
  import org.apache.spark.streaming.scheduler.RMConstants._

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

    val stateSpace = StateSpace(constants)

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) * (MaximumLatency / LatencyGranularity)
    assert(stateSpace.size == expectedSpaceSize)
  }

  test("bestActionFor") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = StateSpace(constants)
    import constants._

    assert(BestReward == stateSpace(State(MinimumExecutors, CoarseMinimumLatency - 1))(NoAction))
    assert(BestReward == stateSpace(State(12, 150))(ScaleOut))
    assert(BestReward == stateSpace(State(12, 1))(ScaleIn))
    assert(NoReward == stateSpace(State(MaximumExecutors, CoarseTargetLatency))(ScaleOut))
    assert(BestReward == stateSpace(State(MaximumExecutors, CoarseTargetLatency))(NoAction))
    assert(BestReward == stateSpace(State(MaximumExecutors - 1, CoarseTargetLatency))(ScaleOut))
    assert(NoReward == stateSpace(State(20, CoarseTargetLatency - 1))(ScaleOut))
    assert(NoReward == stateSpace(State(20, CoarseTargetLatency - 1))(ScaleIn))
    assert(BestReward == stateSpace(State(20, CoarseTargetLatency - 1))(NoAction))
  }
}
