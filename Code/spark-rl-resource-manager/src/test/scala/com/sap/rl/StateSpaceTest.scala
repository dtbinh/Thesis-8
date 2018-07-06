package com.sap.rl

import com.sap.rl.TestCommons._
import com.sap.rl.rm.Action._
import com.sap.rl.rm.{RMConstants, State, StateSpace}
import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class StateSpaceTest extends FunSuite {

  var sparkConf: SparkConf = generateSparkConf()

  test("testInitialization") {
    val constants: RMConstants = RMConstants(sparkConf)
    import constants._

    val stateSpace = StateSpace(constants)

    val expectedSpaceSize: Long = (MaximumExecutors - MinimumExecutors + 1) *
                                  (MaximumLatency / LatencyGranularity) *
                                  (MaximumIncomingMessages / IncomingMessagesGranularity)
    assert(stateSpace.size == expectedSpaceSize)
  }

  test("bestActionFor") {
    val constants: RMConstants = RMConstants(sparkConf)
    val stateSpace = StateSpace(constants)
    import constants._

    assert(BestReward == stateSpace(State(MinimumExecutors, CoarseMinimumLatency - 1, 10))(NoAction))
    assert(BestReward == stateSpace(State(12, 150, 10))(ScaleOut))
    assert(BestReward == stateSpace(State(12, 1, 10))(ScaleIn))
    assert(NoReward == stateSpace(State(MaximumExecutors, CoarseTargetLatency, 10))(ScaleOut))
    assert(BestReward == stateSpace(State(MaximumExecutors, CoarseTargetLatency, 10))(NoAction))
    assert(BestReward == stateSpace(State(MaximumExecutors - 1, CoarseTargetLatency, 10))(ScaleOut))
    assert(NoReward == stateSpace(State(20, CoarseTargetLatency - 1, 10))(ScaleOut))
    assert(NoReward == stateSpace(State(20, CoarseTargetLatency - 1, 10))(ScaleIn))
    assert(BestReward == stateSpace(State(20, CoarseTargetLatency - 1, 10))(NoAction))
  }
}
