package com.sap.rm.rl.vi

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.impl.{DefaultPolicy, DefaultReward}
import com.sap.rm.rl.{Policy, RLResourceManager, Reward, StateSpace}
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
  def apply(config: ResourceManagerConfig, ssc: StreamingContext): ValueIterationResourceManager = {
    new ValueIterationResourceManager(
      config, ssc,
      stateSpace = StateSpace(config),
      policy = DefaultPolicy(config),
      reward = DefaultReward(config)
    )
  }
}
