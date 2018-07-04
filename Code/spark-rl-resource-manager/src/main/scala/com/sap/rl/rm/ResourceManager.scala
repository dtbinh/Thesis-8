package com.sap.rl.rm

import com.sap.rl.rm.Action._
import com.sap.rl.rm.LogStatus._
import com.sap.rl.rm.impl.{DefaultPolicy, DefaultReward}
import org.apache.log4j.Logger
import org.apache.spark.streaming.scheduler._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random.shuffle

abstract class ResourceManager extends StreamingListener with ExecutorAllocator {

  protected lazy val sparkContext: SparkContext = streamingContext.sparkContext
  protected lazy val sparkConf: SparkConf = sparkContext.getConf
  protected lazy val stateSpace: StateSpace = createStateSpace
  protected lazy val policy: Policy = createPolicy
  protected lazy val reward: Reward = createReward
  protected val log: Logger
  protected val constants: RMConstants
  protected var streamingStartTime: Long = 0
  protected var lastTimeDecisionMade: Long = 0
  protected var runningSum: Int = 0
  protected var numberOfBatches: Int = 0
  protected var lastState: State = _
  protected var lastAction: Action = _
  protected var currentState: State = _
  protected var rewardForLastAction: Double = 0
  protected var actionToTake: Action = _

  import constants._

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    streamingStartTime = streamingStarted.time

    log.info(s"$STREAM_STARTED -- StartTime = $streamingStartTime")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val info: BatchInfo = batchCompleted.batchInfo

    // check if batch is valid
    if (!isValidBatch(info)) return

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
    currentState = State(numberOfWorkerExecutors, currentLatency)

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

  def isValidBatch(info: BatchInfo): Boolean = {
    val batchTime: Long = info.batchTime.milliseconds

    val isValid: Boolean = if (info.totalDelay.isEmpty) {
      log.info(s"$BATCH_EMPTY -- BatchTime = $batchTime [ms]")
      false
    } else if (batchTime <= (streamingStartTime + StartupWaitTime)) {
      log.info(s"$START_UP -- BatchTime = $batchTime [ms]")
      false
    } else if (batchTime <= (lastTimeDecisionMade + GracePeriod)) {
      log.info(s"$GRACE_PERIOD -- BatchTime = $batchTime [ms]")
      false
    } else {
      log.info(s"$BATCH_OK -- BatchTime = $batchTime [ms]")
      true
    }

    isValid
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
    val killed: Seq[String] = removeExecutors(shuffle(workerExecutors).take(one))
    log.info(s"$EXEC_KILL_OK -- Killed = $killed")
  }

  def scaleOut(): Unit = {
    if (addExecutors(one)) log.info(s"$EXEC_ADD_OK")
    else log.error(s"$EXEC_ADD_ERR")
  }

  def noAction(): Unit = log.info(s"$EXEC_NO_ACTION")

  def whatIsTheNextAction(): Action = policy.nextActionFrom(lastState, lastAction, currentState)

  def calculateRewardFor(): Double = reward.forAction(lastState, lastAction, currentState)

  def specialize(): Unit = {}

  def start(): Unit = log.info("Started resource manager")

  def stop(): Unit = log.info("Stopped resource manager")

  def createStateSpace: StateSpace = StateSpace(constants)

  def createPolicy: Policy = DefaultPolicy(constants, stateSpace)

  def createReward: Reward = DefaultReward(constants, stateSpace)
}