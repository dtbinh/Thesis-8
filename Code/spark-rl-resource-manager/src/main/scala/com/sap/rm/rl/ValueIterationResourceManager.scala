package com.sap.rm.rl

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.URI

import com.sap.rm.rl.Action.Action
import com.sap.rm.{ResourceManager, ResourceManagerConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable.{HashMap => MutableHashMap, HashSet => MutableHashSet}

class ValueIterationResourceManager(
                                     cfg: ResourceManagerConfig,
                                     ssc: StreamingContext,
                                     stateSpaceOpt: Option[StateSpace] = None,
                                     policyOpt: Option[Policy] = None,
                                     rewardOpt: Option[Reward] = None,
                                     executorStrategyOpt: Option[ExecutorStrategy] = None
                                   ) extends TemporalDifferenceResourceManager(cfg, ssc, stateSpaceOpt, policyOpt, rewardOpt, executorStrategyOpt) {

  import config._
  import logger._

  private lazy val stateActionCount = MutableHashMap[(State, Action), Int]().withDefaultValue(0)
  private lazy val stateActionReward = MutableHashMap[(State, Action), Double]().withDefaultValue(0)
  private lazy val stateActionStateCount = MutableHashMap[(State, Action, State), Int]().withDefaultValue(0)
  private lazy val stateActionRewardBar = MutableHashMap[(State, Action), Double]().withDefaultValue(0)
  private lazy val stateActionStateBar = MutableHashMap[(State, Action, State), Double]().withDefaultValue(0)
  private lazy val startingStates = MutableHashSet[State]()
  private lazy val landingStates = MutableHashSet[State]()
  private lazy val VValues = MutableHashMap[State, Double]().withDefaultValue(0)

  private lazy val uri: URI = URI.create("hdfs:///ssd068126ss/ss.dat")
  private lazy val stateSpacePath = new Path(uri)
  private lazy val hdfsConf: Configuration = new Configuration()
  private lazy val fs: FileSystem = FileSystem.get(uri, hdfsConf)

  private var learningPhaseIsDone: Boolean = false
  private var firstRun: Boolean = true

  override lazy val stateSpace: StateSpace = {
    if (!ValueIterationLearningPhase) {
      // load from HDFS
      val in: ObjectInputStream = new ObjectInputStream(fs.open(stateSpacePath))
      val ss = in.readObject().asInstanceOf[StateSpace]
      in.close()

      ss
    } else {
      stateSpaceOpt.getOrElse(StateSpaceInitializer.getInstance(config).initialize(StateSpace()))
    }
  }

  override def updateStateSpace(): Unit = {
    if (ValueIterationLearningPhase) {
      if (firstRun) {
        // delete old file and create new
        if (fs.exists(stateSpacePath)) {
          fs.delete(stateSpacePath, false)
        }
        firstRun = false
        logHDFSCleanup()
      }

      if (learningPhaseIsDone) {
        // do nothing and issue warning here
        logLearningPhaseIsDone()
        return
      }

      // initialize table
      if (currentBatch.batchTime.milliseconds - streamingStartTime > ValueIterationInitializationTime) {
        stateActionReward.foreach { kv =>
          val (stateAction: (State, Action), reward: Double) = kv
          stateActionRewardBar(stateAction) = reward / stateActionCount(stateAction)
        }

        stateActionStateCount.foreach { kv =>
          val (stateActionState: (State, Action, State), count: Int) = kv
          val stateAction = (stateActionState._1, stateActionState._2)
          val startingState = stateActionState._1
          val landingState = stateActionState._3

          stateActionStateBar(stateActionState) = count.toDouble / stateActionCount(stateAction)
          startingStates += startingState
          landingStates += landingState
        }

        logEverything(
          stateActionCount = stateActionCount,
          stateActionReward = stateActionReward,
          stateActionStateCount = stateActionStateCount,
          stateActionRewardBar = stateActionRewardBar,
          stateActionStateBar = stateActionStateBar,
          startingStates = startingStates,
          landingStates = landingStates)

        0 to ValueIterationInitializationCount foreach { _ =>
          stateActionCount.foreach { kv =>
            val (stateAction: (State, Action), _) = kv
            val (startingState, action) = stateAction

            var sum: Double = 0
            val startingStateVValue = VValues(startingState)
            for (landingState <- landingStates) {
              sum += stateActionStateBar((startingState, action, landingState)) * startingStateVValue
            }

            val QVal: Double = stateActionRewardBar(stateAction) + DiscountFactor * sum
            stateSpace.updateQValueForAction(startingState, action, QVal)
          }

          startingStates.foreach(s => VValues(s) = stateSpace(s).qValues.maxBy(_._2)._2)
          logVValues(VValues)
        }

        stateActionCount.clear()
        stateActionReward.clear()
        stateActionStateCount.clear()
        stateActionRewardBar.clear()
        stateActionStateBar.clear()
        startingStates.clear()
        landingStates.clear()
        VValues.clear()

        // write to hdfs here
        val out: ObjectOutputStream = new ObjectOutputStream(fs.create(stateSpacePath, true))
        out.writeObject(stateSpace)
        out.close()

        // learning phase is done
        logStateSpaceIsStored()
        learningPhaseIsDone = true
      } else {
        stateActionCount((lastState, lastAction)) += 1
        stateActionReward((lastState, lastAction)) += rewardForLastAction
        stateActionStateCount((lastState, lastAction, currentState)) += 1

        logStateActionState(
          lastState = lastState,
          lastAction = lastAction,
          rewardForLastAction = rewardForLastAction,
          currentState = currentState,
          stateActionCount = stateActionCount((lastState, lastAction)),
          stateActionReward = stateActionReward((lastState, lastAction)),
          stateActionStateCount = stateActionStateCount((lastState, lastAction, currentState))
        )
      }

      return
    }

    // update q-values similar to Q-Learning strategy
    super.updateStateSpace()
  }
}

object ValueIterationResourceManager {
  def apply(config: ResourceManagerConfig,
            streamingContext: StreamingContext,
            stateSpaceOpt: Option[StateSpace] = None,
            policyOpt: Option[Policy] = None,
            rewardOpt: Option[Reward] = None,
            executorStrategyOpt: Option[ExecutorStrategy] = None
           ): ResourceManager = {
    new ValueIterationResourceManager(config, streamingContext, stateSpaceOpt, policyOpt, rewardOpt, executorStrategyOpt)
  }
}
