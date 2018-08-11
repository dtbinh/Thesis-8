package com.sap.rm.rl

import java.lang.Math.min

import com.sap.rm.ResourceManager
import Action._
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListenerStreamingStarted}

import scala.util.Random.shuffle

abstract class RLResourceManager extends ResourceManager {

  protected val stateSpace: StateSpace
  protected val policy: Policy
  protected val reward: Reward

  protected var runningSum: Int = 0
  protected var numberOfBatches: Int = 0
  protected var lastState: State = _
  protected var lastAction: Action = _
  protected var currentState: State = _
  protected var rewardForLastAction: Double = 0
  protected var actionToTake: Action = _
  protected var lastTimeDecisionMade: Long = 0
  protected var incomingMessages: Int = 0
  protected var lastAverageIncomingMessages: Int = 0
  import config._

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    super.onStreamingStarted(streamingStarted)
    requestTotalExecutors(MaximumExecutors)
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
    incomingMessages += info.numRecords.toInt

    if (numberOfBatches == WindowSize) {
      // take average
      val currentAverageLatency: Int = runningSum / (WindowSize * LatencyGranularity)
      val currentAverageIncomingMessages: Int = incomingMessages / WindowSize
      logWindowIsFull(currentAverageLatency, currentAverageIncomingMessages)

      // reset
      runningSum = 0
      numberOfBatches = 0
      incomingMessages = 0

      // build the state variable
      var loadIsIncreasing: Boolean = false
      if (currentAverageIncomingMessages > lastAverageIncomingMessages) {
        loadIsIncreasing = true
      }
      currentState = State(numberOfActiveExecutors, currentAverageLatency, loadIsIncreasing)
      lastAverageIncomingMessages = currentAverageIncomingMessages

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
    val executorsToKill: Int = min(ExecutorGranularity, all.size - MinimumExecutors)
    val killed: Int = removeExecutors(shuffle(all).take(executorsToKill)).size
    logScaleInAction(killed)
  }

  def scaleOut(): Unit = {
    val total = numberOfActiveExecutors
    val executorToAdd: Int = min(ExecutorGranularity, MaximumExecutors - total)
    if (requestTotalExecutors(executorToAdd + total)) logScaleOutOK(executorToAdd)
    else logScaleOutError()
  }

  def whatIsTheNextAction(): Action = {
    val currentExecutors = currentState.numberOfExecutors
    currentExecutors match {
      case MinimumExecutors => logExecutorNotEnough(currentState)
      case MaximumExecutors => logNoMoreExecutorsLeft(currentState)
      case _ =>
    }
    policy.nextActionFrom(stateSpace, lastState, lastAction, currentState)
  }

  def calculateRewardFor(): Double = reward.forAction(stateSpace, lastState, lastAction, currentState)

  def specialize(): Unit
}
