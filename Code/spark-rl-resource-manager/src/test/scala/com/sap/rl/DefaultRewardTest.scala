package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rl.implicits._
import com.sap.rl.rm.Action._
import com.sap.rl.rm.RMConstants._
import com.sap.rl.rm.impl.DefaultReward
import com.sap.rl.rm.{RMConstants, State, StateSpace}
import com.sap.rl.util.Precision
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfter, FunSuite}

class DefaultRewardTest extends FunSuite with BeforeAndAfter {

  var sparkConf: SparkConf = _

  implicit val p: Precision = Precision()

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

  test("rewardForLowerLatency") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = StateSpace(constants)
    val rewardFunc: DefaultReward = DefaultReward(constants, stateSpace)

    import constants._

    assert(-BestReward ~= rewardFunc.forAction(State(10, 12), ScaleOut, State(9, 15)))
    assert(BestReward ~= rewardFunc.forAction(State(10, 20), ScaleIn, State(9, 10)))
    assert(BestReward / 20 ~= rewardFunc.forAction(State(20, 10), NoAction, State(20, 11)))
    assert(BestReward ~= rewardFunc.forAction(State(10, 12), ScaleOut, State(11, 50)))
  }

  test("rewardForHigherLatency") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = StateSpace(constants)
    val rewardFunc: DefaultReward = DefaultReward(constants, stateSpace)

    import constants._

    assert(BestReward ~= rewardFunc.forAction(State(10, 12), ScaleOut, State(12, 40)))
    assert(-0.121 ~= rewardFunc.forAction(State(10, 12), NoAction, State(10, 41)))
  }
}
