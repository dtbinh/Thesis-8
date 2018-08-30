package com.sap.rm.rl

import java.lang.Math.min

import com.sap.rm.{ResourceManager, ResourceManagerConfig}
import Action._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListenerBatchCompleted}

import scala.util.Random.shuffle

class TemporalDifferenceResourceManager(
                                         cfg: ResourceManagerConfig,
                                         ssc: StreamingContext,
                                         stateSpace: StateSpace,
                                         policy: Policy,
                                         reward: Reward
                                       ) extends ResourceManager {

  override lazy val streamingContext: StreamingContext = ssc
  override lazy val config: ResourceManagerConfig = cfg

  protected var runningSum: Int = 0
  protected var numberOfBatches: Int = 0
  protected var lastState: State = _
  protected var lastAction: Action = _
  protected var currentState: State = _
  protected var rewardForLastAction: Double = 0
  protected var actionToTake: Action = _
  protected var lastTimeDecisionMade: Long = 0
  protected var currentWindowTotalIncomingMessages: Int = 0
  protected var lastBatchIncomingMessages: Int = 0
  protected var lastWindowAverageIncomingMessages: Int = 0
  protected var currentBatch: BatchInfo = _
  protected var currentWindowIncreasingLoadCount: Int = 0
  import config._

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    processBatch(batchCompleted.batchInfo)
  }

  override def processBatch(info: BatchInfo): Boolean = {
    if (!super.processBatch(info)) return false

    currentBatch = info
    val batchTime = info.batchTime.milliseconds
    if (inGracePeriod(batchTime)) return false

    runningSum += info.processingDelay.get.toInt
    numberOfBatches += 1
    currentWindowTotalIncomingMessages += info.numRecords.toInt
    if (info.numRecords.toInt > lastBatchIncomingMessages) {
      currentWindowIncreasingLoadCount += 1
    } else if (info.numRecords.toInt < lastBatchIncomingMessages) {
      currentWindowIncreasingLoadCount -= 1
    }

    if (numberOfBatches == WindowSize) {
      // take average
      val currentWindowAverageLatency: Int = runningSum / (WindowSize * LatencyGranularity)
      val currentWindowAverageIncomingMessages: Int = currentWindowTotalIncomingMessages / WindowSize
      logWindowIsFull(currentWindowAverageLatency, currentWindowAverageIncomingMessages)

      // reset
      runningSum = 0
      numberOfBatches = 0
      currentWindowTotalIncomingMessages = 0

      // build the state variable
      var loadIsIncreasing: Boolean = false
      if (currentWindowAverageIncomingMessages > lastWindowAverageIncomingMessages) {
        loadIsIncreasing = true
      }
      currentState = State(numberOfActiveExecutors, currentWindowAverageLatency, loadIsIncreasing)
      lastWindowAverageIncomingMessages = currentWindowAverageIncomingMessages

      // do nothing and just initialize to no action
      if (lastState == null) {
        actionToTake = NoAction
        logFirstWindowInitialized()
      } else {
        // calculate reward
        rewardForLastAction = calculateRewardFor()

        // take new action
        actionToTake = whatIsTheNextAction()

        // update state space
        updateStateSpace()

        // request change
        reconfigure(actionToTake)
      }

      // store current state and action
      lastState = currentState
      lastAction = actionToTake
      currentWindowIncreasingLoadCount = 0
      setDecisionTime()
    } else {
      logElementAddedToWindow(runningSum, numberOfBatches)
    }

    lastBatchIncomingMessages = currentWindowTotalIncomingMessages
    true
  }

  def inGracePeriod(batchTime: Long): Boolean = {
    if (batchTime <= (lastTimeDecisionMade + GracePeriod)) {
      logGracePeriod(batchTime)
      return true
    }
    false
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

  def calculateRewardFor(): Double = reward.forAction(stateSpace, lastState, lastAction, currentState).get

  def updateStateSpace(): Unit = {
    val oldQVal: Double = stateSpace(lastState)(lastAction)
    val currentStateQVal: Double = stateSpace(currentState)(actionToTake)

    val newQVal: Double = ((1 - LearningFactor) * oldQVal) + (LearningFactor * (rewardForLastAction + (DiscountFactor * currentStateQVal)))
    stateSpace.updateQValueForAction(lastState, lastAction, newQVal)

    logQValueUpdate(lastState, lastAction, oldQVal, rewardForLastAction, currentState, actionToTake, currentStateQVal, newQVal)
  }
}

object TemporalDifferenceResourceManager {
  def apply(config: ResourceManagerConfig,
            streamingContext: StreamingContext,
            stateSpace: Option[StateSpace] = None,
            policy: Option[Policy] = None,
            reward: Option[Reward] = None
           ): ResourceManager = {

    new TemporalDifferenceResourceManager(
      config,
      streamingContext,
      stateSpace.getOrElse(StateSpaceInitializer.getInstance(config).initialize(StateSpace())),
      policy.getOrElse(PolicyFactory.getPolicy(config)),
      reward.getOrElse(RewardFactory.getReward(config)))
  }
}
