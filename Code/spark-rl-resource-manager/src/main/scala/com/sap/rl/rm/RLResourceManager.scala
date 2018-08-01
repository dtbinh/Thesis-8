package com.sap.rl.rm

import java.lang.Math.min
import com.sap.rl.rm.Action._
import com.sap.rl.rm.impl.{DefaultPolicy, DefaultReward}
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListenerStreamingStarted}

import scala.util.Random.shuffle

abstract class RLResourceManager extends ResourceManager {

  protected lazy val stateSpace: StateSpace = createStateSpace
  protected lazy val policy: Policy = createPolicy
  protected lazy val reward: Reward = createReward
  protected var runningSum: Int = 0
  protected var numberOfBatches: Int = 0
  protected var lastState: State = _
  protected var lastAction: Action = _
  protected var currentState: State = _
  protected var rewardForLastAction: Double = 0
  protected var actionToTake: Action = _
  protected var lastTimeDecisionMade: Long = 0
  import config._

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    super.onStreamingStarted(streamingStarted)
    requestMaximumExecutors(MaximumExecutors)
    logSparkMaxExecutorsRequested(numberOfActiveExecutors)
  }

  def inGracePeriod(batchTime: Long): Boolean = {
    if (batchTime <= (lastTimeDecisionMade + GracePeriod)) {
      logGracePeriod(batchTime)
      return true
    }
    false
  }

  override def processBatch(info: BatchInfo): Boolean = {
    if (!super.processBatch(info)) return false

    // check if batch is valid
    val batchTime = info.batchTime.milliseconds
    if (inGracePeriod(batchTime)) return false

    runningSum += info.processingDelay.get.toInt
    numberOfBatches += 1

    if (numberOfBatches == WindowSize) {
      logWindowIsFull(runningSum, numberOfBatches)

      // take average
      val averageLatency: Int = runningSum / (numberOfBatches * LatencyGranularity)

      // reset
      runningSum = 0
      numberOfBatches = 0

      // build the state variable
      currentState = State(numberOfActiveExecutors, averageLatency)

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
      logElementAddedToWindow(runningSum, numberOfBatches)
    }

    true
  }

  def init(): Unit = {
    lastState = currentState
    lastAction = NoAction

    logFirstWindowInitialized()
    setDecisionTime()
  }

  def setDecisionTime(): Unit = {
    lastTimeDecisionMade = System.currentTimeMillis()
    logDecisionTime(lastTimeDecisionMade)
  }

  def reconfigure(actionToTake: Action): Unit = actionToTake match {
    case ScaleIn => scaleIn()
    case ScaleOut => scaleOut()
    case _ =>
  }

  def scaleIn(): Unit = {
    val all = activeExecutors
    val executorsToKill: Int = min(ExecutorChangePerStep, all.size - MinimumExecutors)
    val killed: Int = removeExecutors(shuffle(all).take(executorsToKill)).size
    logScaleInAction(killed)
  }

  def scaleOut(): Unit = {
    val executorToAdd: Int = min(ExecutorChangePerStep, MaximumExecutors - numberOfActiveExecutors)
    if (addExecutors(executorToAdd)) logScaleOutOK()
    else logScaleOutError()
  }

  def whatIsTheNextAction(): Action = {
    val currentExecutors = currentState.numberOfExecutors
    currentExecutors match {
      case MinimumExecutors => logExecutorNotEnough(currentState)
      case MaximumExecutors => logNoMoreExecutorsLeft(currentState)
      case _ =>
    }
    policy.nextActionFrom(lastState, lastAction, currentState)
  }

  def calculateRewardFor(): Double = reward.forAction(lastState, lastAction, currentState)

  def createStateSpace: StateSpace = StateSpace(config)

  def createPolicy: Policy = DefaultPolicy(config, stateSpace)

  def createReward: Reward = DefaultReward(config, stateSpace)

  def specialize(): Unit
}
