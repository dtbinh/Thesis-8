package org.apache.spark.streaming.scheduler

import com.sap.rl.rm.Action._
import com.sap.rl.rm.LogStatus._
import com.sap.rl.rm.impl.{DefaultPolicy, DefaultReward}
import com.sap.rl.rm.{Policy, Reward, State, StateSpace}
import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext, SparkException}

import scala.util.Random.shuffle

abstract class ResourceManager(constants: RMConstants, streamingContext: StreamingContext) extends StreamingListener {

  import constants._

  // Interface for manipulating number of executors
  protected lazy val executorAllocator: ExecutorAllocationClient = sparkContext.schedulerBackend match {
    case backend: ExecutorAllocationClient => backend
    case _ =>
      throw new SparkException(
        """|Dynamic resource allocation doesn't work in local mode. Please consider using
           |scheduler extending CoarseGrainedSchedulerBackend (such as Spark Standalone,
           |YARN or Mesos).""".stripMargin
      )
  }
  @transient private lazy val log = LogManager.getLogger(this.getClass)
  protected val sparkConf: SparkConf = streamingContext.conf
  protected val sparkContext: SparkContext = streamingContext.sparkContext
  protected val batchDuration: Long = streamingContext.graph.batchDuration.milliseconds
  protected val stateSpace: StateSpace = createStateSpace
  protected val policy: Policy = createPolicy
  protected val reward: Reward = createReward
  protected var streamingStartTime: Long = 0
  protected var lastTimeDecisionMade: Long = 0
  protected var runningSum: Int = 0
  protected var numberOfBatches: Int = 0
  protected var lastState: State = _
  protected var lastAction: Action = _
  protected var currentState: State = _
  protected var rewardForLastAction: Double = 0
  protected var actionToTake: Action = _

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
    val killed: Seq[String] = executorAllocator.killExecutors(shuffle(workerExecutors).take(one))
    log.info(s"$EXEC_KILL_OK -- Killed = $killed")
  }

  def workerExecutors: Seq[String] = activeExecutors.diff(receiverExecutors)

  def scaleOut(): Unit = {
    if (executorAllocator.requestExecutors(one)) log.info(s"$EXEC_ADD_OK")
    else log.error(s"$EXEC_ADD_ERR")
  }

  def noAction(): Unit = log.info(s"$EXEC_NO_ACTION")

  def whatIsTheNextAction(): Action = policy.nextActionFrom(lastState, lastAction, currentState)

  def calculateRewardFor(): Double = reward.forAction(lastState, lastAction, currentState)

  def numberOfWorkerExecutors: Int = numberOfActiveExecutors - numberOfReceiverExecutors

  def numberOfActiveExecutors: Int = activeExecutors.size

  def activeExecutors: Seq[String] = executorAllocator.getExecutorIds()

  def numberOfReceiverExecutors: Int = receiverExecutors.size

  def receiverExecutors: Seq[String] = streamingContext.scheduler.receiverTracker.allocatedExecutors.values.flatten.toSeq

  def start(): Unit = log.info("Started resource manager")

  def stop(): Unit = log.info("Stopped resource manager")

  def createStateSpace: StateSpace = StateSpace(constants)

  def createPolicy: Policy = DefaultPolicy(constants, stateSpace)

  def createReward: Reward = DefaultReward(constants, stateSpace)

  def specialize(): Unit = { }
}