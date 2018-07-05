package com.sap.rl

import com.sap.rl.TestCommons.{DynamicResourceAllocationEnabledKey, DynamicResourceAllocationTestingKey}
import com.sap.rl.rm.Action._
import com.sap.rl.rm.RMConstants._
import com.sap.rl.rm.impl.DefaultPolicy
import com.sap.rl.rm.{Policy, RMConstants, State, StateSpace}
import com.sap.rl.util.Precision
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfter, FunSuite}

class DefaultPolicyTest extends FunSuite with BeforeAndAfter {

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

  test("testBestActionWithLatency") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = StateSpace(constants)
    val policy: Policy = DefaultPolicy(constants, stateSpace)

    import constants._

    assert(ScaleIn != policy.nextActionFrom(State(12, 16), ScaleOut, State(12, 17)))
    assert(ScaleOut != policy.nextActionFrom(State(10, 5), ScaleIn, State(10, 4)))
    assert(ScaleIn == policy.nextActionFrom(State(12, 10), ScaleIn, State(11, 10)))
    assert(ScaleIn != policy.nextActionFrom(State(MinimumExecutors, 10), NoAction, State(MinimumExecutors, 11)))
  }
}
