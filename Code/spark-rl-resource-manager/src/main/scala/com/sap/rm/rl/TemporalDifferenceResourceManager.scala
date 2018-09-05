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

  import config._
  import logger._

  protected lazy val totalDelayWindow = CountBasedSlidingWindow(WindowSize)
  protected lazy val incomingMessageWindow = CountBasedSlidingWindow(WindowSize)
  protected var lastState: State = _
  protected var lastAction: Action = _
  protected var currentState: State = _
  protected var rewardForLastAction: Double = 0
  protected var actionToTake: Action = _
  protected var lastTimeDecisionMade: Long = 0
  protected var lastWindowAverageIncomingMessage: Int = 0
  protected var currentBatch: BatchInfo = _

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    processBatch(batchCompleted.batchInfo)
  }

  override def processBatch(info: BatchInfo): Boolean = {
    if (!super.processBatch(info)) return false

    currentBatch = info
    val batchTime = info.batchTime.milliseconds

    totalDelayWindow.add(info.totalDelay.get.toInt)
    incomingMessageWindow.add(info.numRecords.toInt)

    if (lastTimeDecisionMade + DecisionInterval <= batchTime) {
      // take average
      val currentWindowAverageTotalDelay: Int = (totalDelayWindow.average() / LatencyGranularity).toInt
      val currentWindowAverageIncomingMessage: Int = (incomingMessageWindow.average() / IncomingMessageGranularity).toInt

      // build the state variable
      val loadIsIncreasing = if (currentWindowAverageIncomingMessage > lastWindowAverageIncomingMessage) true else false
      currentState = State(numberOfActiveExecutors, currentWindowAverageTotalDelay, loadIsIncreasing)

      // do nothing and just initialize to no action
      if (lastState == null) {
        actionToTake = NoAction
      } else {
        // calculate reward
        rewardForLastAction = calculateReward()
        // take new action
        actionToTake = whatIsTheNextAction()
        // update state space
        updateStateSpace()
      }
      // request change
      reconfigure(actionToTake)
      // store current state and action, then reset everything
      lastState = currentState
      lastAction = actionToTake
      lastWindowAverageIncomingMessage = currentWindowAverageIncomingMessage
      lastTimeDecisionMade = batchTime
    }

    true
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

  def calculateReward(): Double = reward.forAction(stateSpace, lastState, lastAction, currentState).get

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
