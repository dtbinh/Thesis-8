package com.sap.rl.rm

import com.sap.rl.rm.Action._
import com.sap.rl.rm.LogStatus._
import com.sap.rl.rm.RMConstants._
import com.sap.rl.rm.impl.{DefaultPolicy, DefaultReward}
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListenerBatchCompleted}

import scala.util.Random.shuffle

trait RLResourceManager extends ResourceManager {

  protected lazy val stateSpace: StateSpace = createStateSpace
  protected lazy val policy: Policy = createPolicy
  protected lazy val reward: Reward = createReward
  protected var runningSum: Int = 0
  protected var numberOfBatches: Int = 0
  protected var incomingMessages: Int = 0
  protected var lastState: State = _
  protected var lastAction: Action = _
  protected var currentState: State = _
  protected var rewardForLastAction: Double = 0
  protected var actionToTake: Action = _
  protected var lastTimeDecisionMade: Long = 0

  import constants._

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val info: BatchInfo = batchCompleted.batchInfo

    // check if batch is valid
    if (isInvalidBatch(info)) return

    runningSum = runningSum + info.totalDelay.get.toInt
    numberOfBatches = numberOfBatches + 1
    incomingMessages = incomingMessages + info.numRecords.toInt

    if (numberOfBatches < WindowSize) {
      log.info(s"$WINDOW_ADDED -- (RunningSum,NumberOfBatches,IncomingMessages) = ($runningSum,$numberOfBatches,$incomingMessages)")
      return
    }
    log.info(s"$WINDOW_FULL -- (RunningSum,NumberOfBatches,IncomingMessages) = ($runningSum,$numberOfBatches,$incomingMessages)")

    // take average
    val currentNumberOfExecutors: Int = numberOfWorkerExecutors
    val currentLatency: Int = runningSum / (numberOfBatches * LatencyGranularity)
    val currentIncomingMessages: Int = incomingMessages / (numberOfBatches * IncomingMessagesGranularity)

    // reset
    runningSum = 0
    numberOfBatches = 0
    incomingMessages = 0

    // build the state variable
    currentState = State(currentNumberOfExecutors, currentLatency, currentIncomingMessages)
    if (isInvalidState(currentState)) return

    // count and log SLO violations
    logAndCountSLOViolation(info)

    // do nothing and just initialize to no action
    if (lastState == null) {
      init()
      return
    }

    // calculate reward
    rewardForLastAction = calculateRewardFor()

    // take new action
    actionToTake = whatIsTheNextAction()

    // specialize algorithm
    specialize()

    // request change
    reconfigure(actionToTake)

    // store current state and action
    lastState = currentState
    lastAction = actionToTake

    setDecisionTime()
  }

  def logAndCountSLOViolation(info: BatchInfo): Unit = {
    // log current and last state, as well
    if (super.isSLOViolated(info)) {
      super.incrementSLOViolations()
      log.warn(
        s""" --- $SLO_VIOLATION ---
           | SLOViolations=${numberOfSLOViolations.get()}
           | lastState=$lastState
           | lastAction=$lastAction
           | currentState=$currentState""".stripMargin)
    }
  }

  def isInvalidBatch(info: BatchInfo): Boolean = {
    val batchTime: Long = info.batchTime.milliseconds

    if (info.processingDelay.isEmpty) {
      log.warn(s"$BATCH_EMPTY -- BatchTime = $batchTime [ms]")
      IsInvalid
    } else if (batchTime <= (streamingStartTime + StartupWaitTime)) {
      log.info(s"$START_UP -- BatchTime = $batchTime [ms]")
      IsInvalid
    } else if (batchTime <= (lastTimeDecisionMade + GracePeriod)) {
      log.info(s"$GRACE_PERIOD -- BatchTime = $batchTime [ms]")
      IsInvalid
    } else {
      log.info(s"$BATCH_OK -- BatchTime = $batchTime [ms]")
      IsValid
    }
  }

  def isInvalidState(state: State): Boolean = if (state.numberOfExecutors > MaximumExecutors) {
    log.warn(s"$INVALID_STATE_EXCESSIVE_EXECUTORS -- $state")
    IsInvalid
  } else if (state.latency >= CoarseMaximumLatency) {
    log.warn(s"$INVALID_STATE_EXCESSIVE_LATENCY -- $state")
    IsInvalid
  } else if (state.incomingMessages >= CoarseMaximumIncomingMessages) {
    log.warn(s"$INVALID_STATE_EXCESSIVE_INCOMING_MESSAGES -- $state")
    IsInvalid
  } else {
    log.info(s"$STATE_OK -- $state")
    IsValid
  }

  def init(): Unit = {
    lastState = currentState
    lastAction = NoAction

    log.info(s"$FIRST_WINDOW -- Initialized")
    setDecisionTime()
  }

  def setDecisionTime(): Unit = {
    lastTimeDecisionMade = System.currentTimeMillis()
    log.info(s"$DECIDED -- LastTimeDecisionMade = $lastTimeDecisionMade")
  }

  def reconfigure(actionToTake: Action): Unit = actionToTake match {
    case ScaleIn => scaleIn()
    case ScaleOut => scaleOut()
    case NoAction => noAction()
  }

  def scaleIn(): Unit = {
    val killed: Seq[String] = removeExecutors(shuffle(workerExecutors).take(One))
    log.info(s"$EXEC_KILL_OK -- Killed = $killed")
  }

  def scaleOut(): Unit = {
    if (addExecutors(One)) log.info(s"$EXEC_ADD_OK")
    else log.error(s"$EXEC_ADD_ERR")
  }

  def noAction(): Unit = log.info(s"$EXEC_NO_ACTION")

  def whatIsTheNextAction(): Action = policy.nextActionFrom(lastState, lastAction, currentState)

  def calculateRewardFor(): Double = reward.forAction(lastState, lastAction, currentState)

  def createStateSpace: StateSpace = StateSpace(constants)

  def createPolicy: Policy = DefaultPolicy(constants, stateSpace)

  def createReward: Reward = DefaultReward(constants, stateSpace)

  def specialize(): Unit
}
