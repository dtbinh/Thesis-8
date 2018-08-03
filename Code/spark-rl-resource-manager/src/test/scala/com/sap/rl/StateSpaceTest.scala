package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rm.rl.Action._
import com.sap.rm.ResourceManagerConfig
import com.sap.rm.ResourceManagerConfig._
import com.sap.rm.rl.{State, StateSpace}
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class StateSpaceTest extends FunSuite {

  val sparkConf: SparkConf = createSparkConf()

  test("testZeroInitialization") {
    sparkConf.set(InitializationModeKey, "zero")

    val config: ResourceManagerConfig = createConfig(sparkConf)
    val stateSpace = StateSpace(config)
    import config._

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) * (MaximumLatency / LatencyGranularity)
    assert(stateSpace.size == expectedSpaceSize)
    assert(NoReward == stateSpace(State(MaximumExecutors, MinimumLatency))(ScaleOut))
    assert(NoReward == stateSpace(State(20, CoarseTargetLatency - 1))(ScaleOut))
    assert(NoReward == stateSpace(State(20, CoarseTargetLatency - 1))(ScaleIn))
  }

  test("testRandomInitialization") {
    sparkConf.set(InitializationModeKey, "random")

    val config: ResourceManagerConfig = createConfig(sparkConf)
    val stateSpace = StateSpace(config)
    import config._

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) * (MaximumLatency / LatencyGranularity)
    assert(stateSpace.size == expectedSpaceSize)
  }

  test("testInitialization") {
    sparkConf.set(InitializationModeKey, "optimal")
    val config: ResourceManagerConfig = createConfig(sparkConf)
    val stateSpace = StateSpace(config)
    import config._

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) * (MaximumLatency / LatencyGranularity)
    assert(stateSpace.size == expectedSpaceSize)
  }

  test("bestActionFor") {
    sparkConf.set(InitializationModeKey, "optimal")
    val config: ResourceManagerConfig = createConfig(sparkConf)
    val stateSpace = StateSpace(config)
    import config._

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
