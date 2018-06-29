package com.sap.rl.rm.td

import com.sap.rl.rm.Action.Action
import com.sap.rl.rm.{Action, State}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._

import scala.util.Random.shuffle

class TDResourceManager(constants: RMConstants, streamingContext: StreamingContext) extends ResourceManager(constants, streamingContext) {

  var lastTimeDecisionMade: Long = 0
  var runningSum: Int = 0
  var numberOfBatches: Int = 0

  var lastState: State = _
  var lastTakenAction: Action = _

  val stateSpace = TDStateSpace(constants)
  val policy = TDPolicy(constants, stateSpace)
  val reward = TDReward(constants, stateSpace)

  import constants._
  import com.sap.rl.rm.LogStatus._

  override val listener: StreamingListener = new BatchListener {

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      val info: BatchInfo = batchCompleted.batchInfo
      val batchTime: Long = info.batchTime.milliseconds

      if (info.totalDelay.isEmpty) {
        log.info(s"$BATCH_EMPTY -- BatchTime = $batchTime [ms]")
      } else {
        log.info(s"$BATCH_OK -- BatchTime = $batchTime [ms]")

        if (batchTime > (streamingStartTime + StartupWaitTime)) {
          log.info(s"$NOT_START_UP -- BatchTime = $batchTime [ms]")

          // if we are not in grace period, check for window size
          if (batchTime > (lastTimeDecisionMade + GracePeriod)) {
            log.info(s"$NOT_GRACE_PERIOD -- BatchTime = $batchTime [ms]")

            runningSum = runningSum + info.totalDelay.get.toInt
            numberOfBatches += 1

            if (numberOfBatches == WindowSize) {
              log.info(s"$WINDOW_FULL -- (RunningSum,NumberOfBatches)=($runningSum,$numberOfBatches)")

              val avg = runningSum.toDouble / numberOfBatches
              val normalizedAverageLatency = (avg / LatencyGranularity).toInt

              runningSum = 0
              numberOfBatches = 0

              // build the state variable
              val currentState = State(TDResourceManager.this.numberOfWorkerExecutors, normalizedAverageLatency)

              // do nothing and just initialize to no action
              if (lastState == null) {
                lastState = currentState
                lastTakenAction = Action.NoAction

                log.info(s"$FIRST_WINDOW -- Initialized")
              } else {
                // calculate reward
                val rewardForLastAction: Double = calculateRewardFor(lastState, lastTakenAction, currentState)

                // take new action
                val actionToTake = whatIsTheNextActionFor(lastState, lastTakenAction, currentState)

                // update QValue for last state
                updateQValue(lastState, lastTakenAction, rewardForLastAction, currentState, actionToTake)

                // request change
                reconfigure(actionToTake)

                // store current state and action
                lastState = currentState
                lastTakenAction = actionToTake
              }

              lastTimeDecisionMade = System.currentTimeMillis()
              log.info(s"$DECIDED -- LastTimeDecisionMade = $lastTimeDecisionMade")
            } else {
              log.info(s"$NOT_WINDOW_FULL -- WindowSize = $numberOfBatches")
            }
          } else {
            log.info(s"$GRACE_PERIOD -- BatchTime = $batchTime [ms]")
          }
        } else {
          log.info(s"$START_UP -- BatchTime = $batchTime [ms]")
        }
      }
    }
  }

  private def reconfigure(actionToTake: Action): Unit = {
    if (actionToTake == Action.ScaleIn) {
      if (numberOfWorkerExecutors - ExecutorGranularity >= MinimumLatency) {
        val killed: Seq[String] = executorAllocator.killExecutors(shuffle(workerExecutors).take(ExecutorGranularity))
        log.info(s"$EXEC_KILL_OK -- Killed = $killed")
      } else {
        log.error(s"$EXEC_KILL_NOT_ENOUGH")
      }
    } else if (actionToTake == Action.ScaleOut) {
      if (numberOfWorkerExecutors + ExecutorGranularity <= MaximumExecutors) {
        val opResult: Boolean = executorAllocator.requestExecutors(ExecutorGranularity)
        if (opResult) {
          log.info(s"$EXEC_ADD_OK")
        } else {
          log.error(s"$EXEC_ADD_ERR")
        }
      } else {
        log.error(s"$EXEC_ADD_EXCESSIVE")
      }
    } else {
      log.info(s"$EXEC_NO_ACTION")
    }
  }

  private def whatIsTheNextActionFor(lastState: State, lastAction: Action, currentState: State): Action = {
    policy.nextActionFrom(lastState, lastAction, currentState)
  }

  private def calculateRewardFor(lastState: State, lastAction: Action, currentState: State): Double = {
    reward.forAction(lastState, lastAction, currentState)
  }

  private def updateQValue(lastState: State, lastAction: Action, rewardForLastAction: Double, currentState: State, actionToTake: Action): Unit = {
    val oldQVal: Double = stateSpace(lastState)(lastAction)
    val currentStateQVal: Double = stateSpace(currentState)(actionToTake)

    val newQVal: Double = ((1 - LearningFactor) * oldQVal) + (LearningFactor * (rewardForLastAction + (DiscountFactor * currentStateQVal)))
    stateSpace.updateQValueForAction(lastState, lastAction, newQVal)

    log.info(s""" --- QValue-Update-Begin ---
                | ==========================
                | lastState=$lastState
                | lastAction=$lastAction
                | oldQValue=$oldQVal
                | reward=$rewardForLastAction
                | ==========================
                | currentState=$currentState
                | actionTotake=$actionToTake
                | currentStateQValue=$currentStateQVal
                | ==========================
                | newQValue=$newQVal
                | ==========================
                | --- QValue-Update-End ---""".stripMargin)
  }
}

object TDResourceManager {
  def apply(ssc: StreamingContext): TDResourceManager = {
    val constants: RMConstants = RMConstants(ssc.sparkContext.getConf)
    new TDResourceManager(constants, ssc)
  }
}

