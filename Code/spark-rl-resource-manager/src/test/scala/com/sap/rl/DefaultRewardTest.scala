package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rl.implicits._
import com.sap.rl.rm.Action._
import com.sap.rl.rm.impl.DefaultReward
import com.sap.rl.rm.{ResourceManagerConfig, State, StateSpace}
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class DefaultRewardTest extends FunSuite {

  val sparkConf: SparkConf = createSparkConf()
  val constants: ResourceManagerConfig = ResourceManagerConfig(sparkConf)

  test("rewardForLowerLatency") {
    val stateSpace = StateSpace(constants)
    val rewardFunc: DefaultReward = DefaultReward(constants, stateSpace)

    assert(rewardFunc.forAction(State(10, 12, 10), ScaleOut, State(11, 15, 10)) ~= 45.4545)
    assert(rewardFunc.forAction(State(10, 20, 10), ScaleIn, State(9, 10, 10)) ~= 66.6666)
    assert(rewardFunc.forAction(State(20, 10, 10), NoAction, State(20, 11, 10)) ~= 29)
  }

  test("rewardForHigherLatency") {
    val stateSpace = StateSpace(constants)
    val rewardFunc: DefaultReward = DefaultReward(constants, stateSpace)

    assert(rewardFunc.forAction(State(10, 12, 10), ScaleOut, State(11, 50, 10)) ~= 11)
    assert(rewardFunc.forAction(State(10, 12, 10), ScaleIn, State(9, 40, 10)) ~= -1)
    assert(rewardFunc.forAction(State(10, 12, 10), NoAction, State(10, 41, 10)) ~= -2)
  }
}
