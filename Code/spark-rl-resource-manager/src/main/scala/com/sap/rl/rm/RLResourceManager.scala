package com.sap.rl.rm

import com.sap.rl.rm.Action._
import com.sap.rl.rm.LogStatus._
import com.sap.rl.rm.RMConstants._
import com.sap.rl.rm.impl.{DefaultPolicy, DefaultReward}
import org.apache.log4j.Logger
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListenerStreamingStarted}

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
  protected val log: Logger

  import constants._

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    super.onStreamingStarted(streamingStarted)
    log.info(s"$MY_TAG -- $SPARK_MAX_EXEC -- Before = $numberOfActiveExecutors")
    requestMaximumExecutors()
    log.info(s"$MY_TAG -- $SPARK_MAX_EXEC -- After = $numberOfActiveExecutors")
  }

  def inGracePeriod(batchTime: Long): Boolean = {
    if (batchTime <= (lastTimeDecisionMade + GracePeriod)) {
      log.debug(s"$MY_TAG -- $GRACE_PERIOD -- BatchTime = $batchTime [ms]")
      return true
    }
    false
  }

  override def processBatch(info: BatchInfo): Boolean = {
    if (!super.processBatch(info)) return false

    // check if batch is valid
    val batchTime = info.batchTime.milliseconds
    if (inGracePeriod(batchTime)) return false

    runningSum = runningSum + info.processingDelay.get.toInt
    numberOfBatches = numberOfBatches + 1
    incomingMessages = incomingMessages + info.numRecords.toInt

    if (numberOfBatches == WindowSize) {
      log.info(s"$MY_TAG -- $WINDOW_FULL -- (RunningSum,NumberOfBatches,IncomingMessages) = ($runningSum,$numberOfBatches,$incomingMessages)")

      // take average
      val averageLatency: Int = runningSum / (numberOfBatches * LatencyGranularity)
      val averageIncomingMessages: Int = incomingMessages / (numberOfBatches * IncomingMessagesGranularity)

      // reset
      runningSum = 0
      numberOfBatches = 0
      incomingMessages = 0

      // build the state variable
      currentState = State(numberOfActiveExecutors, averageLatency, averageIncomingMessages)

      // do nothing and just initialize to no action
      if (lastState == null) {
        init()
      } else {
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
    } else {
      log.debug(s"$MY_TAG -- $WINDOW_ADDED -- (RunningSum,NumberOfBatches,IncomingMessages) = ($runningSum,$numberOfBatches,$incomingMessages)")
    }

    true
  }

  def init(): Unit = {
    lastState = currentState
    lastAction = NoAction

    log.info(s"$MY_TAG -- $FIRST_WINDOW -- Initialized")
    setDecisionTime()
  }

  def setDecisionTime(): Unit = {
    lastTimeDecisionMade = System.currentTimeMillis()
    log.info(s"$MY_TAG -- $DECIDED -- LastTimeDecisionMade = $lastTimeDecisionMade")
  }

  def reconfigure(actionToTake: Action): Unit = actionToTake match {
    case ScaleIn => scaleIn()
    case ScaleOut => scaleOut()
    case _ =>
  }

  def scaleIn(): Unit = {
    val killed: Seq[String] = removeExecutors(shuffle(activeExecutors).take(One))
    log.info(s"$MY_TAG -- $EXEC_KILL_OK -- Killed = $killed")
  }

  def scaleOut(): Unit = {
    if (addExecutors(One)) log.info(s"$MY_TAG -- $EXEC_ADD_OK")
    else log.error(s"$MY_TAG -- $EXEC_ADD_ERR")
  }

  def whatIsTheNextAction(): Action = policy.nextActionFrom(lastState, lastAction, currentState)

  def calculateRewardFor(): Double = reward.forAction(lastState, lastAction, currentState)

  def createStateSpace: StateSpace = StateSpace(constants)

  def createPolicy: Policy = DefaultPolicy(constants, stateSpace)

  def createReward: Reward = DefaultReward(constants, stateSpace)

  def specialize(): Unit
}
