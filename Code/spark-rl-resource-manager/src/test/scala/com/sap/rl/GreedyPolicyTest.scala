package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rm.rl.Action._
import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.impl.policy.GreedyPolicy
import com.sap.rm.rl.{Policy, State, StateSpace, StateSpaceInitializer}
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class GreedyPolicyTest extends FunSuite {

  val sparkConf: SparkConf = createSparkConf()
  val config: ResourceManagerConfig = createConfig(sparkConf)
  import config._

  test("testBestActionWithLatency") {
    val stateSpace = StateSpaceInitializer.getInstance(config).initialize(StateSpace())
    val policy: Policy = GreedyPolicy(config)

    assert(ScaleIn != policy.nextActionFrom(stateSpace, State(12, 16, loadIsIncreasing = true), ScaleOut, State(12, 17, loadIsIncreasing = true)))
    assert(ScaleOut != policy.nextActionFrom(stateSpace, State(10, 5, loadIsIncreasing = false), ScaleIn, State(10, 4, loadIsIncreasing = false)))
    assert(ScaleIn == policy.nextActionFrom(stateSpace, State(12, 10, loadIsIncreasing = false), ScaleIn, State(11, 10, loadIsIncreasing = false)))
    assert(ScaleIn != policy.nextActionFrom(stateSpace, State(MinimumExecutors, 10, loadIsIncreasing = true), NoAction, State(MinimumExecutors, 11, loadIsIncreasing = true)))
  }
}
