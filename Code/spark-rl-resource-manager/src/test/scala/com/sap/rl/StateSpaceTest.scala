package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rl.rm.Action._
import com.sap.rl.rm.{ResourceManagerConfig, State, StateSpace}
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class StateSpaceTest extends FunSuite {

  val sparkConf: SparkConf = createSparkConf()
  val constants: ResourceManagerConfig = createRMConstants(sparkConf)
  import constants._

  test("testInitialization") {
    val stateSpace = StateSpace(constants)

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) *
                                  (MaximumLatency / LatencyGranularity)
    assert(stateSpace.size == expectedSpaceSize)
  }

  test("bestActionFor") {
    val stateSpace = StateSpace(constants)

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
