package com.sap.rl.rm.td

import com.sap.rl.rm.{Action, State}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.scheduler.RMConstants
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.streaming.scheduler.RMConstants._
import com.sap.rl.TestCommons._
import com.sap.rl.implicits._
import com.sap.rl.util.Precision

class TDRewardTest extends FunSuite with BeforeAndAfter {

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
    val stateSpace = TDStateSpace(constants)
    val rewardFunc: TDReward = TDReward(constants, stateSpace)

    val reward: Double = rewardFunc.forAction(State(10, 12), Action.ScaleIn, State(9, 15))
    val expectedReward: Double = 1.toDouble / 9

    assert(expectedReward ~= reward)
  }

  test("rewardForHigherLatency") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = TDStateSpace(constants)
    val rewardFunc: TDReward = TDReward(constants, stateSpace)

    val reward: Double = rewardFunc.forAction(State(10, 12), Action.ScaleOut, State(12, 40))
    val expectedReward: Double = 0

    assert(expectedReward ~= reward)
  }

}
