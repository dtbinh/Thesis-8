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

object ResourceManagerLogger {

  @transient private lazy val log: Logger = Logger("RMLogs")
  @transient private lazy val statLog: Logger = Logger("Stat")
  private var correlationId: Int = 0

  private var config: ResourceManagerConfig = _

  def apply(cfg: ResourceManagerConfig): ResourceManagerLogger.type = {
    if (config == null) {
      synchronized {
        if (config == null) {
          config = cfg
        }

      }
    }
    this
  }

  def incrementCorrelationId(): Unit = correlationId += 1

  protected lazy val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Europe/Berlin"))
  import dateFormatter._

  def logExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    log.info("{} - {} - Time = {}", SPARK_EXEC_ADDED, correlationId, format(ofEpochMilli(executorAdded.time)))
  }

  def logExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    log.info("{} - {} - Time = {}", SPARK_EXEC_REMOVED, correlationId, format(ofEpochMilli(executorRemoved.time)))
  }

  def logApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info("{} - {} - Time = {}", APP_ENDED, correlationId, format(ofEpochMilli(applicationEnd.time)))
  }

  def logStreamingStarted(streamingStarted: StreamingListenerStreamingStarted, numExecutors: Int): Unit = {
    log.info("{} - {} - (Time,Workers) = ({},{})", STREAMING_STARTED, correlationId, format(ofEpochMilli(streamingStarted.time)), numExecutors)
  }

  def logEmptyBatch(batchTime: Long): Unit = {
    log.warn("{} - {} - BatchTime = {}", BATCH_EMPTY, correlationId, format(ofEpochMilli(batchTime)))
  }

  def logStartupIgnoreBatch(batchTime: Long): Unit = {
    log.warn("{} - {} - BatchTime = {}", BATCH_STARTUP, correlationId, format(ofEpochMilli(batchTime)))
  }

  def logExcessiveProcessingTime(processingTime: Long): Unit = {
    log.warn("{} - {} - ProcessingTime = {}", EXCESSIVE_LATENCY, correlationId, processingTime)
  }

  def logStat(stat: Stat): Unit = {
    log.info("{} - {} - {}", STAT, correlationId, stat)
    statLog.info("{} - {}", correlationId, stat)
  }

  def logScaleInAction(numberOfKilledExecutors: Int): Unit = {
    log.info("{} - {} - Killed = {}", EXEC_KILL_OK, correlationId, numberOfKilledExecutors)
  }

  def logScaleOutOK(requestedExecutors: Int): Unit = {
    log.info("{} - {} - RequestedExecutors = {}", EXEC_ADD_OK, correlationId, requestedExecutors)
  }

  def logScaleOutError(): Unit = {
    log.warn("{} - {}", EXEC_ADD_ERR, correlationId)
  }

  def logQValueUpdate(lastState: State, lastAction: Action, oldQVal: Double, rewardForLastAction: Double, currentState: State, actionToTake: Action, currentStateQVal: Double, newQVal: Double): Unit = {
    log.info(
      s""" --- QValue-Update-Begin ---
         | correlationId: $correlationId
         | currentState=$currentState --- actionTotake=$actionToTake
         | lastState=$lastState --- lastAction=$lastAction
         | reward=$rewardForLastAction --- currentStateQValue=$currentStateQVal
         | oldQValue=$oldQVal --- newQValue=$newQVal""".stripMargin)
  }

  def logExecutorNotEnough(currentState: State): Unit = {
    log.warn("{} - {} - currentState = {}", REMOVED_ACTION_SCALE_IN, correlationId, currentState)
  }

  def logNoMoreExecutorsLeft(currentState: State): Unit = {
    log.warn("{} - {} - currentState = {}", REMOVED_ACTION_SCALE_OUT, correlationId, currentState)
  }

  def logStateActionState(lastState: State, lastAction: Action, rewardForLastAction: Double, currentState: State,
                          stateActionCount: Int, stateActionReward: Double, stateActionStateCount: Int): Unit = {
    log.info(
      s""" --- State-Action-State-Stat ---
         | correlationId: $correlationId
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
         | correlationId: $correlationId
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
        | correlationId: $correlationId
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
    log.info("{} - {} - (generatedRandom,epsilon,action) = ({},{},{})", tag, correlationId, generatedRandom.formatted("%.2f"), epsilon.formatted("%.2f"), action)
  }

  def logVisitedState(state: State, qValues: Seq[(Action, Double)]): Unit = {
    log.info("{} - {} - (state,qValues) = ({},{})", VISITED_STATE, correlationId, state, qValues)
  }

  def logUnvisitedState(state: State, qValues: Seq[(Action, Double)]): Unit = {
    log.info("{} - {} - (state,qValues) = ({},{})", UNVISITED_STATE, correlationId, state, qValues)
  }

  def logWaitingListLength(length: Int): Unit = {
    log.info("{} - length = {}", correlationId, length)
  }
}
