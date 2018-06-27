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
  val policy = TDPolicy(stateSpace)
  val reward = TDReward(constants, stateSpace)

  import constants._

  override val listener: StreamingListener = new BatchListener {

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      val info: BatchInfo = batchCompleted.batchInfo
      val batchTime: Long = info.batchTime.milliseconds

      // log everything
      log.info(s"Received successful batch ${batchTime} ms with completion time")
      log.info(info.toString)

      if (info.totalDelay.isEmpty) {
        log.info(s"Batch ${batchTime} -- IS empty")
      } else {
        log.info(s"Batch ${batchTime} -- ISN'T empty")

        if (batchTime > (streamingStartTime + StartupWaitTime)) {
          log.info(s"Batch ${batchTime} -- AFTER startup phase")

          // if we are not in grace period, check for window size
          if (batchTime > (lastTimeDecisionMade + GracePeriod)) {
            log.info(s"Batch ${batchTime} -- NOT IN grace period")

            runningSum = runningSum + info.totalDelay.get.toInt
            numberOfBatches += 1

            log.info(s"runningSum: ${runningSum}, numberOfBatches: ${numberOfBatches}")

            if (numberOfBatches == WindowSize) {
              val avg = runningSum.toDouble / numberOfBatches
              val normalizedAverageLatency = (avg / LatencyGranularity).toInt

              runningSum = 0
              numberOfBatches = 0

              // build the state variable
              val currentState = State(TDResourceManager.this.numberOfWorkerExecutors, normalizedAverageLatency)

              log.info(s"Current state -- ${currentState}")

              // do nothing and just initialize to no action
              if (lastState == null) {
                lastState = currentState
                lastTakenAction = Action.NoAction

                log.info("First window completed -- initialized")
              } else {
                // calculate reward
                val rewardForLastAction: Double = calculateRewardFor(lastState, lastTakenAction, currentState)

                // update QValue for last state
                updateQValue(lastState, lastTakenAction, rewardForLastAction)

                log.info(s"updated [[ ${lastState}, ${lastTakenAction} ]] with reward ${rewardForLastAction}")

                // take new action
                val actionToTake = whatIsTheNextActionFor(currentState)

                log.info(s"taking action ${actionToTake} for ${currentState}")

                // request change
                reconfigure(actionToTake)

                // store current state and action
                lastState = currentState
                lastTakenAction = actionToTake

                log.info("stored cuurent state/action")
              }

              lastTimeDecisionMade = System.currentTimeMillis()
              log.info(s"set lastTimeDecisionMade to ${lastTimeDecisionMade}")
            } else {
              log.info(s"window not full yet. window size: ${numberOfBatches}")
            }
          } else {
            log.info(s"Batch ${batchTime} -- IN grace period")
          }
        } else {
          log.info(s"Batch ${batchTime} -- BEFORE startup phase")
        }
      }
    }
  }

  private def reconfigure(action: Action): Unit = {
    if (action == Action.ScaleIn) {
      executorAllocator.killExecutors(shuffle(workerExecutors).take(ExecutorGranularity))
    } else if (action == Action.ScaleOut){
      executorAllocator.requestExecutors(ExecutorGranularity)
    } else {
      // do nothing, log
    }
  }

  private def whatIsTheNextActionFor(state: State): Action = {
    policy.nextActionFrom(state)
  }

  private def calculateRewardFor(lastState: State, lastAction: Action, currentState: State): Double = {
    reward.forAction(lastState, lastAction, currentState)
  }

  private def updateQValue(state: State, action: Action, expectedReward: Double): Unit = {
    stateSpace.updateQValueForAction(state, action, expectedReward)
  }
}

object TDResourceManager {
  def apply(ssc: StreamingContext): TDResourceManager = {
    val constants: RMConstants = RMConstants(ssc.sparkContext.getConf)
    new TDResourceManager(constants, ssc)
  }
}

