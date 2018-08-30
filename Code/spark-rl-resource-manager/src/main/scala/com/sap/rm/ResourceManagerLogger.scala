package com.sap.rm

import java.time.Instant.ofEpochMilli
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.sap.rm.rl.Action.Action
import LogTags._
import com.sap.rm.rl.State
import com.typesafe.scalalogging.Logger
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.apache.spark.streaming.scheduler.StreamingListenerStreamingStarted
import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}

trait ResourceManagerLogger {

  @transient private[ResourceManagerLogger] lazy val log: Logger = Logger("RMLogs")
  @transient private[ResourceManagerLogger] lazy val statLog: Logger = Logger("Stat")
  protected def isDebugEnabled: Boolean

  protected lazy val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Europe/Berlin"))
  import dateFormatter._

  def logExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    log.info("{} - Time = {}", SPARK_EXEC_ADDED, format(ofEpochMilli(executorAdded.time)))
  }

  def logExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    log.info("{} - Time = {}", SPARK_EXEC_REMOVED, format(ofEpochMilli(executorRemoved.time)))
  }

  def logApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info("{} - Time = {}", APP_ENDED, format(ofEpochMilli(applicationEnd.time)))
  }

  def logStreamingStarted(streamingStarted: StreamingListenerStreamingStarted, numExecutors: Int): Unit = {
    log.info("{} - (Time,Workers) = ({},{})", STREAMING_STARTED, format(ofEpochMilli(streamingStarted.time)), numExecutors)
  }

  def logEmptyBatch(batchTime: Long): Unit = {
    log.warn("{} - BatchTime = {}", BATCH_EMPTY, format(ofEpochMilli(batchTime)))
  }

  def logStartupIgnoreBatch(batchTime: Long): Unit = {
    log.warn("{} - BatchTime = {}", BATCH_STARTUP, format(ofEpochMilli(batchTime)))
  }

  def logExcessiveProcessingTime(processingTime: Long): Unit = {
    log.warn("{} - ProcessingTime = {}", EXCESSIVE_LATENCY, processingTime)
  }

  def logBatchOK(batchTime: Long): Unit = {
    if (isDebugEnabled) {
      log.info("{} - BatchTime = {}", BATCH_OK, format(ofEpochMilli(batchTime)))
    }
  }

  def logStat(stat: Stat): Unit = {
    log.info("{} - {}", STAT, stat)
    statLog.info("{}", stat)
  }

  def logGracePeriod(batchTime: Long): Unit = {
    if (isDebugEnabled) {
      log.info("{} - BatchTime = {}", GRACE_PERIOD, format(ofEpochMilli(batchTime)))
    }
  }

  def logWindowIsFull(currentWindowAverageProcessingTime: Int, currentWindowIncomingMessage: Int): Unit = {
    log.info("{} - (CurrentWindowAverageProcessingTime,CurrentWindowIncomingMessage) = ({},{})", WINDOW_FULL, currentWindowAverageProcessingTime, currentWindowIncomingMessage)
  }

  def logFirstWindowInitialized(): Unit = {
    log.info("{} - Initialized", FIRST_WINDOW)
  }

  def logScaleInAction(numberOfKilledExecutors: Int): Unit = {
    log.info("{} - Killed = {}", EXEC_KILL_OK, numberOfKilledExecutors)
  }

  def logScaleOutOK(requestedExecutors: Int): Unit = {
    log.info("{} - RequestedExecutors = {}", EXEC_ADD_OK, requestedExecutors)
  }

  def logScaleOutError(): Unit = {
    log.warn("{}", EXEC_ADD_ERR)
  }

  def logQValueUpdate(lastState: State, lastAction: Action, oldQVal: Double, rewardForLastAction: Double, currentState: State, actionToTake: Action, currentStateQVal: Double, newQVal: Double): Unit = {
    log.info(
      s""" --- QValue-Update-Begin ---
         | currentState=$currentState
         | actionTotake=$actionToTake
         | lastState=$lastState
         | lastAction=$lastAction
         | reward=$rewardForLastAction
         | currentStateQValue=$currentStateQVal
         | oldQValue=$oldQVal
         | newQValue=$newQVal
         | --- QValue-Update-End ---""".stripMargin)
  }

  def logExecutorNotEnough(currentState: State): Unit = {
    log.warn("{} - currentState = {}", REMOVED_ACTION_SCALE_IN, currentState)
  }

  def logNoMoreExecutorsLeft(currentState: State): Unit = {
    log.warn("{} - currentState = {}", REMOVED_ACTION_SCALE_OUT, currentState)
  }

  def logStateActionState(lastState: State, lastAction: Action, rewardForLastAction: Double, currentState: State,
                          stateActionCount: Int, stateActionReward: Double, stateActionStateCount: Int): Unit = {
    log.info(
      s""" --- State-Action-State-Stat ---
         | lastState=$lastState
         | lastAction=$lastAction
         | reward=$rewardForLastAction
         | currentState=$currentState
         | stateActionCount=$stateActionCount
         | stateActionReward=$stateActionReward
         | stateActionStateCount=$stateActionStateCount""".stripMargin)
  }

  def logEverything(stateActionCount: MutableMap[(State, Action), Int], stateActionReward: MutableMap[(State, Action), Double],
                    stateActionStateCount: MutableMap[(State, Action, State), Int], stateActionRewardBar: MutableMap[(State, Action), Double],
                    stateActionStateBar: MutableMap[(State, Action, State), Double], startingStates: MutableSet[State],
                    landingStates: MutableSet[State]): Unit = {
    log.info(
      s"""
         | stateActionCount: $stateActionCount
         | stateActionReward: $stateActionReward
         | stateActionStateCount: $stateActionStateCount
         | stateActionRewardBar: $stateActionRewardBar
         | stateActionStateBar: $stateActionStateBar
         | startingStates: $startingStates
         | landingStates: $landingStates
       """.stripMargin
    )
  }

  def logVValues(vvalues: MutableMap[State, Double]): Unit = {
    log.info(
      s"""
        | vvalues: $vvalues
      """.stripMargin)
  }

  def logRandomAction(generatedRandom: Double, epsilon: Double, action: Action): Unit = {
    logAction(RANDOM_ACTION, generatedRandom, epsilon, action)
  }

  def logOptimalAction(generatedRandom: Double, epsilon: Double, action: Action): Unit = {
    logAction(OPTIMAL_ACTION, generatedRandom, epsilon, action)
  }

  private def logAction(tag: Status, generatedRandom: Double, epsilon: Double, action: Action): Unit = {
    log.info("{} - (generatedRandom, epsilon, action) = ({},{},{})", tag, generatedRandom.formatted("%.2f"), epsilon.formatted("%.2f"), action)
  }
}
