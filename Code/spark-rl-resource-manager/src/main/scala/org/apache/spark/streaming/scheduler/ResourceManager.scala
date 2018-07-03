package org.apache.spark.streaming.scheduler

import com.sap.rl.rm.Action.{Action, NoAction, ScaleIn, ScaleOut}
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
  val sparkConf: SparkConf = streamingContext.conf
  val sparkContext: SparkContext = streamingContext.sparkContext
  val batchDuration: Long = streamingContext.graph.batchDuration.milliseconds
  val stateSpace: StateSpace = createStateSpace
  val policy: Policy = createPolicy
  val reward: Reward = createReward
  var streamingStartTime: Long = 0
  var lastTimeDecisionMade: Long = 0

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    streamingStartTime = streamingStarted.time

    log.info(s"$STREAM_STARTED -- StartTime = $streamingStartTime")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val info: BatchInfo = batchCompleted.batchInfo
    val batchTime: Long = info.batchTime.milliseconds

    if (info.totalDelay.isEmpty) {
      log.info(s"$BATCH_EMPTY -- BatchTime = $batchTime [ms]")
      return
    }
    if (batchTime <= (streamingStartTime + StartupWaitTime)) {
      log.info(s"$START_UP -- BatchTime = $batchTime [ms]")
      return
    }
    if (batchTime <= (lastTimeDecisionMade + GracePeriod)) {
      log.info(s"$GRACE_PERIOD -- BatchTime = $batchTime [ms]")
      return
    }

    log.info(s"$BATCH_OK -- BatchTime = $batchTime [ms]")
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

  def whatIsTheNextActionFor(lastState: State, lastAction: Action, currentState: State): Action = {
    policy.nextActionFrom(lastState, lastAction, currentState)
  }

  def calculateRewardFor(lastState: State, lastAction: Action, currentState: State): Double = {
    reward.forAction(lastState, lastAction, currentState)
  }

  def start(): Unit = log.info("Started resource manager")

  def stop(): Unit = log.info("Stopped resource manager")

  def numberOfWorkerExecutors: Int = numberOfActiveExecutors - numberOfReceiverExecutors

  def numberOfActiveExecutors: Int = activeExecutors.size

  def activeExecutors: Seq[String] = executorAllocator.getExecutorIds()

  def numberOfReceiverExecutors: Int = receiverExecutors.size

  def receiverExecutors: Seq[String] = streamingContext.scheduler.receiverTracker.allocatedExecutors.values.flatten.toSeq

  def createStateSpace: StateSpace = StateSpace(constants)

  def createPolicy: Policy = DefaultPolicy(constants, stateSpace)

  def createReward: Reward = DefaultReward(constants, stateSpace)
}