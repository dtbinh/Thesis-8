package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rm.rl.Action._
import com.sap.rm.ResourceManagerConfig
import com.sap.rm.ResourceManagerConfig._
import com.sap.rm.rl.{State, StateSpace, StateSpaceInitializer}
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class StateSpaceTest extends FunSuite {

  val sparkConf: SparkConf = createSparkConf()

  test("testZeroInitialization") {
    sparkConf.set(InitializationModeKey, "zero")

    val config: ResourceManagerConfig = createConfig(sparkConf)
    val stateSpace = StateSpaceInitializer.getInstance(config).initialize(StateSpace())
    import config._

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) * (MaximumLatency / LatencyGranularity) * 2
    assert(stateSpace.size == expectedSpaceSize)
    assert(NoReward == stateSpace(State(MaximumExecutors, 1, loadIsIncreasing = true))(ScaleOut))
    assert(NoReward == stateSpace(State(20, CoarseTargetLatency - 1, loadIsIncreasing = false))(ScaleOut))
    assert(NoReward == stateSpace(State(20, CoarseTargetLatency - 1, loadIsIncreasing = true))(ScaleIn))
  }

  test("testRandomInitialization") {
    sparkConf.set(InitializationModeKey, "random")

    val config: ResourceManagerConfig = createConfig(sparkConf)
    val stateSpace = StateSpaceInitializer.getInstance(config).initialize(StateSpace())
    import config._

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) * (MaximumLatency / LatencyGranularity) * 2
    assert(stateSpace.size == expectedSpaceSize)
  }

  test("testInitialization") {
    sparkConf.set(InitializationModeKey, "optimal")
    val config: ResourceManagerConfig = createConfig(sparkConf)
    val stateSpace = StateSpaceInitializer.getInstance(config).initialize(StateSpace())
    import config._

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) * (MaximumLatency / LatencyGranularity) * 2
    assert(stateSpace.size == expectedSpaceSize)
  }

  test("bestActionFor") {
    sparkConf.set(InitializationModeKey, "optimal")
    val config: ResourceManagerConfig = createConfig(sparkConf)
    val stateSpace = StateSpaceInitializer.getInstance(config).initialize(StateSpace())
    import config._

    assert(BestReward == stateSpace(State(MinimumExecutors, 2, loadIsIncreasing = true))(NoAction))
    assert(BestReward == stateSpace(State(12, 10, loadIsIncreasing = true))(ScaleOut))
    assert(0.875 == stateSpace(State(12, 1, loadIsIncreasing = false))(ScaleIn))
    assert(BestReward == stateSpace(State(MaximumExecutors, CoarseTargetLatency, loadIsIncreasing = false))(NoAction))
    assert(BestReward == stateSpace(State(MaximumExecutors - 1, CoarseTargetLatency, loadIsIncreasing = true))(ScaleOut))
    assert(-BestReward == stateSpace(State(20, CoarseTargetLatency - 1, loadIsIncreasing = false))(ScaleOut))
    assert(0.125 == stateSpace(State(20, CoarseTargetLatency - 1, loadIsIncreasing = true))(ScaleIn))
    assert(0.125 == stateSpace(State(20, CoarseTargetLatency - 1, loadIsIncreasing = false))(ScaleIn))
    assert(NoReward == stateSpace(State(20, CoarseTargetLatency - 1, loadIsIncreasing = false))(NoAction))
  }
}
