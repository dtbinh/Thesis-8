package com.sap.rm.rl

import com.sap.rm.{ResourceManager, ResourceManagerConfig}
import Action._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListenerBatchCompleted, StreamingListenerBatchSubmitted}
import scala.util.Random.shuffle

class TemporalDifferenceResourceManager(
                                         cfg: ResourceManagerConfig,
                                         ssc: StreamingContext,
                                         stateSpaceOpt: Option[StateSpace] = None,
                                         policyOpt: Option[Policy] = None,
                                         rewardOpt: Option[Reward] = None,
                                         executorStrategyOpt: Option[ExecutorStrategy] = None
                                       ) extends ResourceManager {

  override lazy val streamingContext: StreamingContext = ssc
  override lazy val config: ResourceManagerConfig = cfg

  import config._
  import logger._

  lazy val targetWaitingListLength: Int = TargetLatency / batchDuration.toInt
  lazy val stateSpace: StateSpace = stateSpaceOpt.getOrElse(StateSpaceInitializer.getInstance(config).initialize(StateSpace()))
  lazy val policy: Policy = policyOpt.getOrElse(PolicyFactory.getPolicy(config))
  lazy val reward: Reward = rewardOpt.getOrElse(RewardFactory.getReward(config))
  lazy val executorStrategy: ExecutorStrategy = executorStrategyOpt.getOrElse(ExecutorStrategyFactory.getExecutorStrategy(config, this))

  protected lazy val totalDelayWindow = CountBasedSlidingWindow(WindowSize)
  protected lazy val incomingMessageWindow = CountBasedSlidingWindow(WindowSize)
  protected lazy val batchWaitingList = BatchWaitingList(targetWaitingListLength)
  protected var lastState: State = _
  protected var lastAction: Action = _
  protected var currentState: State = _
  protected var rewardForLastAction: Double = 0
  protected var actionToTake: Action = _
  protected var lastTimeDecisionMade: Long = 0
  protected var lastWindowAverageIncomingMessage: Int = 0
  protected var currentBatch: BatchInfo = _
  protected var numberOfExecutors: Int = 0

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = batchWaitingList.enqueue(batchSubmitted.batchInfo.submissionTime)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    processBatch(batchCompleted.batchInfo)
  }

  override def processBatch(info: BatchInfo): Boolean = {
    batchWaitingList.dequeue(info.submissionTime)
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
      numberOfExecutors = numberOfActiveExecutors
      currentState = State(currentWindowAverageTotalDelay, loadIsIncreasing)

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
      batchWaitingList.reset()
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
    val executorsToKill: Int = executorStrategy.howManyExecutorsToRemove(all.size)
    val killed: Int = removeExecutors(shuffle(all).take(executorsToKill)).size
    logScaleInAction(killed)
  }

  def scaleOut(): Unit = {
    val total = numberOfActiveExecutors
    val executorToAdd: Int = executorStrategy.howManyExecutorsToAdd(total)
    if (requestTotalExecutors(executorToAdd + total)) logScaleOutOK(executorToAdd)
    else logScaleOutError()
  }

  def whatIsTheNextAction(): Action = policy.nextActionFrom(stateSpace, lastState, lastAction, currentState, batchWaitingList, numberOfExecutors)

  def calculateReward(): Double = reward.forAction(stateSpace, lastState, lastAction, currentState, batchWaitingList, numberOfExecutors).get

  def updateStateSpace(): Unit = {
    val oldQVal: Double = stateSpace(lastState, lastAction)
    val currentStateQVal: Double = stateSpace(currentState, actionToTake)

    val newQVal: Double = ((1 - LearningFactor) * oldQVal) + (LearningFactor * (rewardForLastAction + (DiscountFactor * currentStateQVal)))
    stateSpace.updateQValueForAction(lastState, lastAction, newQVal)

    logQValueUpdate(lastState, lastAction, oldQVal, rewardForLastAction, currentState, actionToTake, currentStateQVal, newQVal)
  }

  def waitingListLength: Int = batchWaitingList.length
}

object TemporalDifferenceResourceManager {
  def apply(config: ResourceManagerConfig,
            streamingContext: StreamingContext,
            stateSpaceOpt: Option[StateSpace] = None,
            policyOpt: Option[Policy] = None,
            rewardOpt: Option[Reward] = None,
            executorStrategyOpt: Option[ExecutorStrategy] = None
           ): ResourceManager = {
    new TemporalDifferenceResourceManager(config, streamingContext, stateSpaceOpt, policyOpt, rewardOpt, executorStrategyOpt)
  }
}
