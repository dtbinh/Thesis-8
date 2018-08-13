package com.sap.rm.rl.vi

import com.sap.rm.{ResourceManager, ResourceManagerConfig}
import com.sap.rm.rl.impl.{DefaultPolicy, DefaultReward}
import com.sap.rm.rl._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted

class ValueIterationResourceManager(
                                     val config: ResourceManagerConfig,
                                     val streamingContext: StreamingContext,
                                     val stateSpace: StateSpace,
                                     val policy: Policy,
                                     val reward: Reward
                                   ) extends RLResourceManager {


  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = synchronized {
    super.onBatchCompleted(batchCompleted)
  }

  def specialize(): Unit = {
    // TODO: implement for VI
  }
}

object ValueIterationResourceManager {
  def apply(config: ResourceManagerConfig,
            streamingContext: StreamingContext,
            stateSpace: Option[StateSpace] = None,
            policy: Option[Policy] = None,
            reward: Option[Reward] = None
           ): ResourceManager = {
    new ValueIterationResourceManager(
      config,
      streamingContext,
      stateSpace.getOrElse { StateSpaceFactory.factoryInstance(config).initialize(StateSpace()) },
      policy.getOrElse(DefaultPolicy(config)),
      reward.getOrElse(DefaultReward(config)))
  }
}
