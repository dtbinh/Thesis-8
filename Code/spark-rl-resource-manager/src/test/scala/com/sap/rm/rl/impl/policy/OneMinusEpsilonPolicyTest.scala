package com.sap.rm.rl.impl.policy

import com.sap.rl.TestCommons.{createConfig, createSparkConf}
import com.sap.rm.ResourceManagerConfig
import com.sap.rm.ResourceManagerConfig._
import com.sap.rm.rl.Action._
import com.sap.rm.rl.{RandomNumberGenerator, State, StateSpace, StateSpaceInitializer}
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class OneMinusEpsilonPolicyTest extends FunSuite {

  val sparkConf: SparkConf = createSparkConf()

  test("testNextActionFrom") {
    sparkConf.set(InitializationModeKey, "zero")
    sparkConf.set(EpsilonKey, "0.3")

    val config: ResourceManagerConfig = createConfig(sparkConf)
    import config._
    val stateSpace = StateSpaceInitializer.getInstance(config).initialize(StateSpace())

    val generator: RandomNumberGenerator = new RandomNumberGenerator {
      var nums = List(0.1, 0.2, 0.3, 0.08, 0.6)
      var bools = List(true, false, true, false)

      override def nextDouble(): Double = {
        val head = nums.head
        nums = nums.tail
        head
      }

      override def nextBoolean(): Boolean = {
        val head = bools.head
        bools = bools.tail
        head
      }
    }

    val policy = OneMinusEpsilonPolicy(config, GreedyPolicy(config), generator)

    assert(policy.nextActionFrom(stateSpace, State(MinimumExecutors, 7, loadIsIncreasing = true), NoAction, State(MinimumExecutors, 8, loadIsIncreasing = true)) == ScaleOut)
    assert(policy.nextActionFrom(stateSpace, State(MaximumExecutors, 7, loadIsIncreasing = true), NoAction, State(MaximumExecutors, 8, loadIsIncreasing = true)) == NoAction)
    assert(policy.nextActionFrom(stateSpace, State(MaximumExecutors, 7, loadIsIncreasing = true), NoAction, State(MaximumExecutors, 8, loadIsIncreasing = true)) == ScaleIn)
    assert(policy.nextActionFrom(stateSpace, State(10, 7, loadIsIncreasing = true), NoAction, State(10, 8, loadIsIncreasing = true)) == NoAction)
    assert(policy.nextActionFrom(stateSpace, State(MaximumExecutors, 7, loadIsIncreasing = true), NoAction, State(MaximumExecutors, 8, loadIsIncreasing = true)) == NoAction)
  }
}
