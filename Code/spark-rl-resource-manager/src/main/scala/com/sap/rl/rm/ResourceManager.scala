package com.sap.rl.rm

import com.sap.rl.rm.Action._
import com.sap.rl.rm.LogStatus._
import com.sap.rl.rm.impl.{DefaultPolicy, DefaultReward}
import org.apache.log4j.Logger
import org.apache.spark.scheduler._
import org.apache.spark.streaming.scheduler._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random.shuffle

trait ResourceManager extends StreamingListener with SparkListenerTrait with ExecutorAllocator {

  protected lazy val sparkContext: SparkContext = streamingContext.sparkContext
  protected lazy val sparkConf: SparkConf = sparkContext.getConf
  protected lazy val stateSpace: StateSpace = createStateSpace
  protected lazy val policy: Policy = createPolicy
  protected lazy val reward: Reward = createReward
  protected val log: Logger
  protected val constants: RMConstants
  protected var streamingStartTime: Option[Long] = None
  protected var lastTimeDecisionMade: Option[Long] = None
  protected var runningSum: Option[Int] = None
  protected var numberOfBatches: Option[Int] = None
  protected var lastState: Option[State] = None
  protected var lastAction: Option[Action] = None
  protected var currentState: Option[State] = None
  protected var rewardForLastAction: Option[Double] = None
  protected var actionToTake: Option[Action] = None
  protected var resourceManagerStopped: Option[Boolean] = None

  import constants._

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    super.onExecutorAdded(executorAdded)

    log.info(s"$SPARK_EXEC_ADDED -- (ID,Time) = (${executorAdded.executorId},${executorAdded.time}")
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    super.onExecutorRemoved(executorRemoved)

    log.info(s"$SPARK_EXEC_REMOVED -- (ID,Time) = (${executorRemoved.executorId},${executorRemoved.time}")
  }

  override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {
    super.onExecutorBlacklisted(executorBlacklisted)

    log.info(s"$SPARK_EXEC_BLACKLISTED -- (ID,Time) = (${executorBlacklisted.executorId},${executorBlacklisted.time}")
  }

  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {
    super.onExecutorUnblacklisted(executorUnblacklisted)

    log.info(s"$SPARK_EXEC_UNBLACKLISTED -- (ID,Time) = (${executorUnblacklisted.executorId},${executorUnblacklisted.time}")
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    super.onApplicationStart(applicationStart)

    resourceManagerStopped = Some(false)
    log.info(s"$APP_STARTED -- ApplicationStartTime = ${applicationStart.time}")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(applicationEnd)

    resourceManagerStopped = Some(true)
    log.info(s"$APP_ENDED -- ApplicationEndTime = ${applicationEnd.time}")
  }

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    if (isResourceManagerStopped) return

    streamingStartTime = Some(streamingStarted.time)
    log.info(s"$STREAMING_STARTED -- StreamingStartTime = $streamingStartTime")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    if (isResourceManagerStopped) return
    if (isStreamingStopped) return

    val info: BatchInfo = batchCompleted.batchInfo

    // check if batch is valid
    if (!isValidBatch(info)) return

    runningSum = Some(runningSum.get + info.totalDelay.get.toInt)
    numberOfBatches = Some(numberOfBatches.get + 1)

    if (numberOfBatches.get < WindowSize) {
      log.info(s"$WINDOW_ADDED -- (RunningSum,NumberOfBatches) = ($runningSum,$numberOfBatches)")
      return
    }
    log.info(s"$WINDOW_FULL -- (RunningSum,NumberOfBatches) = ($runningSum,$numberOfBatches)")

    val currentLatency: Int = runningSum.get / (numberOfBatches.get * LatencyGranularity)

    // reset
    runningSum = Some(0)
    numberOfBatches = Some(0)

    // build the state variable
    currentState = Some(State(numberOfWorkerExecutors, currentLatency))

    // do nothing and just initialize to no action
    if (lastState == null) {
      init()
      return
    }

    // calculate reward
    rewardForLastAction = Some(calculateRewardFor())

    // take new action
    actionToTake = Some(whatIsTheNextAction())

    // specialize algorithm
    specialize()

    // request change
    reconfigure(actionToTake.get)

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
    } else if (batchTime <= (streamingStartTime.get + StartupWaitTime)) {
      log.info(s"$START_UP -- BatchTime = $batchTime [ms]")
      false
    } else if (batchTime <= (lastTimeDecisionMade.get + GracePeriod)) {
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
    lastAction = Some(NoAction)

    log.info(s"$FIRST_WINDOW -- Initialized")
    setDecisionTime()
  }

  def setDecisionTime(): Unit = {
    lastTimeDecisionMade = Some(System.currentTimeMillis())
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

  def whatIsTheNextAction(): Action = policy.nextActionFrom(lastState.get, lastAction.get, currentState.get)

  def calculateRewardFor(): Double = reward.forAction(lastState.get, lastAction.get, currentState.get)

  def specialize(): Unit = {}

  def createStateSpace: StateSpace = StateSpace(constants)

  def createPolicy: Policy = DefaultPolicy(constants, stateSpace)

  def createReward: Reward = DefaultReward(constants, stateSpace)

  def isResourceManagerStopped: Boolean = resourceManagerStopped match {
    case None | Some(false) =>
      log.warn(s"$RM_STOPPED")
      true
    case _ => false
  }

  def isStreamingStopped: Boolean = streamingStartTime match {
    case None =>
      log.warn(s"$STREAMING_STOPPED")
      true
    case _ => false
  }
}