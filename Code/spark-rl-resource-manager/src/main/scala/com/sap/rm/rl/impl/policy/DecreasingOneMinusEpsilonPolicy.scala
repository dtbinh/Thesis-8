package com.sap.rm.rl.impl.policy

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.{Policy, RandomNumberGenerator}

class DecreasingOneMinusEpsilonPolicy(config: ResourceManagerConfig, policy: Policy, generator: RandomNumberGenerator) extends OneMinusEpsilonPolicy(config, policy, generator) {
  import config._

  @transient private var e = Epsilon

  override def epsilon: Double = {
    var tmp: Double = 0
    if (e >= 0) {
      tmp = e
      e = e - EpsilonStep
    }

    tmp
  }
}

object DecreasingOneMinusEpsilonPolicy {
  def apply(config: ResourceManagerConfig, policy: Policy, generator: RandomNumberGenerator): DecreasingOneMinusEpsilonPolicy = new DecreasingOneMinusEpsilonPolicy(config, policy, generator)
}