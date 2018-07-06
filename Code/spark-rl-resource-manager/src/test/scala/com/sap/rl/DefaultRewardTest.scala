package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rl.implicits._
import com.sap.rl.rm.Action._
import com.sap.rl.rm.impl.DefaultReward
import com.sap.rl.rm.{RMConstants, State, StateSpace}
import com.sap.rl.util.Precision
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class DefaultRewardTest extends FunSuite {

  var sparkConf: SparkConf = generateSparkConf()
  implicit val p: Precision = Precision()

  test("rewardForLowerLatency") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = StateSpace(constants)
    val rewardFunc: DefaultReward = DefaultReward(constants, stateSpace)

    import constants._

    assert(-BestReward ~= rewardFunc.forAction(State(10, 12, 10), ScaleOut, State(9, 15, 10)))
    assert(BestReward ~= rewardFunc.forAction(State(10, 20, 10), ScaleIn, State(9, 10, 10)))
    assert(BestReward / 20 ~= rewardFunc.forAction(State(20, 10, 10), NoAction, State(20, 11, 10)))
    assert(BestReward ~= rewardFunc.forAction(State(10, 12, 10), ScaleOut, State(11, 50, 10)))
  }

  test("rewardForHigherLatency") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = StateSpace(constants)
    val rewardFunc: DefaultReward = DefaultReward(constants, stateSpace)

    import constants._

    assert(BestReward ~= rewardFunc.forAction(State(10, 12, 10), ScaleOut, State(12, 40, 10)))
    assert(-0.121 ~= rewardFunc.forAction(State(10, 12, 10), NoAction, State(10, 41, 10)))
  }
}
