package com.sap.rm.rl.impl.policy

import com.sap.rm.{ResourceManagerConfig, ResourceManagerLogger}
import com.sap.rm.rl.Action.{Action, _}
import com.sap.rm.rl._

class OneMinusEpsilonPolicy(config: ResourceManagerConfig, policy: Policy, generator: RandomNumberGenerator) extends Policy {

  import config._

  @transient private lazy val logger = ResourceManagerLogger(config)
  import logger._

  def epsilon: Double = Epsilon

  override def nextActionFrom(stateSpace: StateSpace, lastState: State, lastAction: Action, currentState: State, waitingList: BatchWaitingList, numberOfExecutors: Int): Action = {
    val r = generator.nextDouble()
    val e = epsilon
    var randomAction = false

    // take random action
    val action = if (r > 0 && r <= e) {
      randomAction = true
      if (numberOfExecutors == MinimumExecutors) ScaleOut
      else if (numberOfExecutors == MaximumExecutors) ScaleIn
      else if (r <= e/2) ScaleIn
      else ScaleOut
    } else {
      // take action based on policy
      policy.nextActionFrom(stateSpace, lastState, lastAction, currentState, waitingList, numberOfExecutors)
    }

    if (randomAction) logRandomAction(r, e, action) else logOptimalAction(r, e, action)
    action
  }
}

object OneMinusEpsilonPolicy {
  def apply(config: ResourceManagerConfig, policy: Policy, generator: RandomNumberGenerator): OneMinusEpsilonPolicy = new OneMinusEpsilonPolicy(config, policy, generator)
}
