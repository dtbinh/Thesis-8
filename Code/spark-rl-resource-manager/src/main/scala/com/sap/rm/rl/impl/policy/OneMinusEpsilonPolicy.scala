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
      if (r < e/3) {
        // do nothing
        NoAction
      } else if (r >= e/3 && r < e*2/3) {
        // scale in, if can't be done then choose between NoAction and ScaleOut
        if (numberOfExecutors == MinimumExecutors) {
          if (generator.nextBoolean()) ScaleOut else NoAction
        } else ScaleIn
      } else {
        // scaleout, if can't be done then choose between NoAction and ScaleIn
        if (numberOfExecutors == MaximumExecutors) {
          if (generator.nextBoolean()) ScaleIn else NoAction
        } else ScaleOut
      }
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
