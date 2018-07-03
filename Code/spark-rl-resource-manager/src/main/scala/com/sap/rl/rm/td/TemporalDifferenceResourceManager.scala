package com.sap.rl.rm.td

import com.sap.rl.rm.Action._
import com.sap.rl.rm.LogStatus._
import com.sap.rl.rm.State
import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._

class TemporalDifferenceResourceManager(constants: RMConstants, streamingContext: StreamingContext)
  extends ResourceManager(constants, streamingContext) {

  import constants._

  @transient private lazy val log = LogManager.getLogger(this.getClass)

  var runningSum: Int = 0
  var numberOfBatches: Int = 0

  var lastState: State = _
  var lastTakenAction: Action = _

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    super.onBatchCompleted(batchCompleted)

    val info: BatchInfo = batchCompleted.batchInfo

    runningSum = runningSum + info.totalDelay.get.toInt
    numberOfBatches += 1

    if (numberOfBatches < WindowSize) {
      log.info(s"$WINDOW_ADDED -- (RunningSum,NumberOfBatches)=($runningSum,$numberOfBatches)")
      return
    }

    log.info(s"$WINDOW_FULL -- (RunningSum,NumberOfBatches)=($runningSum,$numberOfBatches)")

    val currentLatency: Int = runningSum / (numberOfBatches * LatencyGranularity)

    // reset
    runningSum = 0
    numberOfBatches = 0

    // build the state variable
    val currentState = State(numberOfWorkerExecutors, currentLatency)

    // do nothing and just initialize to no action
    if (lastState == null) {
      init(currentState)
      return
    }

    // calculate reward
    val rewardForLastAction: Double = calculateRewardFor(lastState, lastTakenAction, currentState)

    // take new action
    val actionToTake = whatIsTheNextActionFor(lastState, lastTakenAction, currentState)

    // update QValue for last state
    updateQValue(lastState, lastTakenAction, rewardForLastAction, currentState, actionToTake)

    // request change
    reconfigure(actionToTake)

    // store current state and action
    lastState = currentState
    lastTakenAction = actionToTake

    setDecisionTime()
  }

  private def init(currentState: State): Unit = {
    lastState = currentState
    lastTakenAction = NoAction

    log.info(s"$FIRST_WINDOW -- Initialized")
    setDecisionTime()
  }

  private def updateQValue(lastState: State, lastAction: Action, rewardForLastAction: Double, currentState: State, actionToTake: Action): Unit = {
    val oldQVal: Double = stateSpace(lastState)(lastAction)
    val currentStateQVal: Double = stateSpace(currentState)(actionToTake)

    val newQVal: Double = ((1 - LearningFactor) * oldQVal) + (LearningFactor * (rewardForLastAction + (DiscountFactor * currentStateQVal)))
    stateSpace.updateQValueForAction(lastState, lastAction, newQVal)

    log.info(
      s""" --- QValue-Update-Begin ---
         | ==========================
         | lastState=$lastState
         | lastAction=$lastAction
         | oldQValue=$oldQVal
         | reward=$rewardForLastAction
         | ==========================
         | currentState=$currentState
         | actionTotake=$actionToTake
         | currentStateQValue=$currentStateQVal
         | ==========================
         | newQValue=$newQVal
         | ==========================
         | --- QValue-Update-End ---""".stripMargin)
  }
}

object TemporalDifferenceResourceManager {
  def apply(ssc: StreamingContext): TemporalDifferenceResourceManager = {
    val constants: RMConstants = RMConstants(ssc.sparkContext.getConf)
    new TemporalDifferenceResourceManager(constants, ssc)
  }
}