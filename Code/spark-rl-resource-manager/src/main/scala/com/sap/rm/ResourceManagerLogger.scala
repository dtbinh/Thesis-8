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

trait ResourceManagerLogger {

  @transient private[ResourceManagerLogger] lazy val log: Logger = Logger("RMLogs")
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
  }

  def logGracePeriod(batchTime: Long): Unit = {
    if (isDebugEnabled) {
      log.info("{} - BatchTime = {}", GRACE_PERIOD, format(ofEpochMilli(batchTime)))
    }
  }

  def logWindowIsFull(runningSum: Int, numberOfBatches: Int): Unit = {
    log.info("{} - (RunningSum,NumberOfBatches) = ({},{})", WINDOW_FULL, runningSum, numberOfBatches)
  }

  def logElementAddedToWindow(runningSum: Int, numberOfBatches: Int): Unit = {
    if (isDebugEnabled) {
      log.info("{} - (RunningSum,NumberOfBatches) = ({},{})", WINDOW_ADDED, runningSum, numberOfBatches)
    }
  }

  def logFirstWindowInitialized(): Unit = {
    log.info("{} - Initialized", FIRST_WINDOW)
  }

  def logDecisionTime(decisionTime: Long): Unit = {
    log.info("{} - LastTimeDecisionMade = {}", DECIDED, format(ofEpochMilli(decisionTime)))
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
}
