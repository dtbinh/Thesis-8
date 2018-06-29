package com.sap.rl.rm.td

import com.sap.rl.TestCommons.{DynamicResourceAllocationEnabledKey, DynamicResourceAllocationTestingKey}
import com.sap.rl.rm.{Action, Policy, State}
import com.sap.rl.util.Precision
import org.apache.spark.SparkConf
import org.apache.spark.streaming.scheduler.RMConstants
import org.apache.spark.streaming.scheduler.RMConstants._
import org.scalatest.{BeforeAndAfter, FunSuite}

class TDPolicyTest extends FunSuite with BeforeAndAfter {

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
//
//  test("testBestActionWithLatency120") {
//    val constants: RMConstants = RMConstants(sparkConf)
//    val stateSpace = TDStateSpace(constants)
//    val policy: Policy = TDPolicy(constants, stateSpace)
//
//    val bestAction = policy.nextActionFrom(State(12, 120))
//    assert(bestAction == Action.ScaleOut)
//  }
//
//  test("testBestActionWithLatency10") {
//    val constants: RMConstants = RMConstants(sparkConf)
//    val stateSpace = TDStateSpace(constants)
//    val policy: Policy = TDPolicy(constants, stateSpace)
//
//    val bestAction = policy.nextActionFrom(State(12, 10))
//    assert(bestAction == Action.ScaleIn)
//  }
}
