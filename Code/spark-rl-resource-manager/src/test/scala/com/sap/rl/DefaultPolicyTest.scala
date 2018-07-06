package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rl.rm.Action._
import com.sap.rl.rm.impl.DefaultPolicy
import com.sap.rl.rm.{Policy, RMConstants, State, StateSpace}
import com.sap.rl.util.Precision
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class DefaultPolicyTest extends FunSuite {

  var sparkConf: SparkConf = generateSparkConf()
  implicit val p: Precision = Precision()

  test("testBestActionWithLatency") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = StateSpace(constants)
    val policy: Policy = DefaultPolicy(constants, stateSpace)

    import constants._

    assert(ScaleIn != policy.nextActionFrom(State(12, 16, 10), ScaleOut, State(12, 17, 10)))
    assert(ScaleOut != policy.nextActionFrom(State(10, 5, 10), ScaleIn, State(10, 4, 10)))
    assert(ScaleIn == policy.nextActionFrom(State(12, 10, 10), ScaleIn, State(11, 10, 10)))
    assert(ScaleIn != policy.nextActionFrom(State(MinimumExecutors, 10, 10), NoAction, State(MinimumExecutors, 11, 10)))
  }
}
