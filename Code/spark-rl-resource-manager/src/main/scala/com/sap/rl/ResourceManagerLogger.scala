package com.sap.rl

import java.time.Instant.ofEpochMilli
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.LogTags._
import com.sap.rl.rm.State
import com.typesafe.scalalogging.Logger
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.apache.spark.streaming.scheduler.StreamingListenerStreamingStarted

trait ResourceManagerLogger {

  private lazy val log: Logger = Logger("RMLogs")
  protected def isDebugEnabled: Boolean

  protected lazy val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Europe/Berlin"))
  import dateFormatter._

  def logApplicationStarted(applicationStart: SparkListenerApplicationStart): Unit = {
    log.info("{} - ApplicationStartTime = {}", APP_STARTED, format(ofEpochMilli(applicationStart.time)))
  }

  def logExecutorAdded(executorAdded: SparkListenerExecutorAdded, numExecutors: Int): Unit = {
    log.info("{} - (Time,Workers) = ({},{})", SPARK_EXEC_ADDED, format(ofEpochMilli(executorAdded.time)), numExecutors)
  }

  def logExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved, numExecutors: Int): Unit = {
    log.info("{} - (Time,Workers) = ({},{})", SPARK_EXEC_REMOVED, format(ofEpochMilli(executorRemoved.time)), numExecutors)
  }

  def logApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info("{} - ApplicationEndTime = {}", APP_ENDED, format(ofEpochMilli(applicationEnd.time)))
  }

  def logStreamingStarted(streamingStarted: StreamingListenerStreamingStarted, numExecutors: Int): Unit = {
    log.info("{} - (StreamingStartTime,Workers) = ({},{})", STREAMING_STARTED, format(ofEpochMilli(streamingStarted.time)), numExecutors)
  }

  def logEmptyBatch(batchTime: Long): Unit = {
    log.warn("{} - BatchTime = {}", BATCH_EMPTY, format(ofEpochMilli(batchTime)))
  }

  def logExcessiveProcessingTime(processingTime: Long): Unit = {
    log.warn("{} - ProcessingTime = {}", EXCESSIVE_LATENCY, processingTime)
  }

  def logExcessiveIncomingMessages(numRecords: Int): Unit = {
    log.warn("{} - IncomingMessages = {}", EXCESSIVE_INCOMING_MESSAGES, numRecords)
  }

  def logBatchOK(batchTime: Long): Unit = {
    if (isDebugEnabled) {
      log.info("{} - BatchTime = {}", BATCH_OK, format(ofEpochMilli(batchTime)))
    }
  }

  def logStat(stat: Stat): Unit = {
    log.info("{} - {}", STAT, stat)
  }

  def logGracePeriod(batchTime: Long): Unit = {
    if (isDebugEnabled) {
      log.info("{} - BatchTime = {}", GRACE_PERIOD, format(ofEpochMilli(batchTime)))
    }
  }

  def logWindowIsFull(runningSum: Int, numberOfBatches: Int, incomingMessages: Int): Unit = {
    log.info("{} - (RunningSum,NumberOfBatches,IncomingMessages) = ({},{},{})", WINDOW_FULL, runningSum, numberOfBatches, incomingMessages)
  }

  def logElementAddedToWindow(runningSum: Int, numberOfBatches: Int, incomingMessages: Int): Unit = {
    if (isDebugEnabled) {
      log.info("{} - (RunningSum,NumberOfBatches,IncomingMessages) = ({},{},{})", WINDOW_ADDED, runningSum, numberOfBatches, incomingMessages)
    }
  }

  def logFirstWindowInitialized(): Unit = {
    log.info("{} - Initialized", FIRST_WINDOW)
  }

  def logDecisionTime(now: Instant): Unit = {
    log.info("{} - LastTimeDecisionMade = {}", DECIDED, format(now))
  }

  def logScaleInAction(numberOfKilledExecutors: Int): Unit = {
    log.info("{} - Killed = {}", EXEC_KILL_OK, numberOfKilledExecutors)
  }

  def logScaleOutOK(): Unit = {
    log.info("{}", EXEC_ADD_OK)
  }

  def logScaleOutError(): Unit = {
    log.warn("{}", EXEC_ADD_ERR)
  }

  def logSparkMaxExecutorsRequested(numExecutors: Int): Unit = {
    log.info("{} - Workers = {}", SPARK_MAX_EXEC, numExecutors)
  }

  def logQValueUpdate(lastState: State, lastAction: Action, oldQVal: Double, rewardForLastAction: Double, currentState: State, actionToTake: Action, currentStateQVal: Double, newQVal: Double): Unit = {
    log.info(
      s""" --- QValue-Update-Begin ---
         | lastState=$lastState
         | lastAction=$lastAction
         | oldQValue=$oldQVal
         | reward=$rewardForLastAction
         | currentState=$currentState
         | actionTotake=$actionToTake
         | currentStateQValue=$currentStateQVal
         | newQValue=$newQVal
         | --- QValue-Update-End ---""".stripMargin)
  }

  def logExecutorNotEnough(lastState: State, lastAction: Action, currentState: State): Unit = {
    log.warn(
      s""" --- $EXEC_NOT_ENOUGH ---
         | lastState=$lastState
         | lastAction=$lastAction
         | currentState=$currentState""".stripMargin)
  }

  def logNoMoreExecutorsLeft(lastState: State, lastAction: Action, currentState: State): Unit = {
    log.warn(
      s""" --- $EXEC_EXCESSIVE ---
         | lastState=$lastState
         | lastAction=$lastAction
         | currentState=$currentState""".stripMargin)
  }
}
