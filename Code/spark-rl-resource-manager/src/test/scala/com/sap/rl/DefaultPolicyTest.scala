package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rm.rl.Action._
import com.sap.rm.rl.impl.DefaultPolicy
import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{Policy, State, StateSpace}
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class DefaultPolicyTest extends FunSuite {

  val sparkConf: SparkConf = createSparkConf()
  val constants: ResourceManagerConfig = createConfig(sparkConf)
  import constants._

  test("testBestActionWithLatency") {
    val stateSpace = StateSpace(constants)
    val policy: Policy = DefaultPolicy(constants, stateSpace)

    assert(ScaleIn != policy.nextActionFrom(State(12, 16, loadIsIncreasing = true), ScaleOut, State(12, 17, loadIsIncreasing = true)))
    assert(ScaleOut != policy.nextActionFrom(State(10, 5, loadIsIncreasing = false), ScaleIn, State(10, 4, loadIsIncreasing = false)))
    assert(ScaleIn == policy.nextActionFrom(State(12, 10, loadIsIncreasing = false), ScaleIn, State(11, 10, loadIsIncreasing = false)))
    assert(ScaleIn != policy.nextActionFrom(State(MinimumExecutors, 10, loadIsIncreasing = true), NoAction, State(MinimumExecutors, 11, loadIsIncreasing = true)))
  }
}
