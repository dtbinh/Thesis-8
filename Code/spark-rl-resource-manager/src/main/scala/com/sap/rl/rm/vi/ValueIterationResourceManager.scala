package com.sap.rl.rm.vi

import com.sap.rl.rm.Action._
import com.sap.rl.rm.State
import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{RMConstants, ResourceManager}

class ValueIterationResourceManager(constants: RMConstants, streamingContext: StreamingContext)
  extends ResourceManager(constants, streamingContext) {

  import constants._

  @transient private lazy val log = LogManager.getLogger(this.getClass)

  var lastState: State = _
  var lastTakenAction: Action = _


}
