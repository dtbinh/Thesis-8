package com.sap.rl.rm.vi

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{RMConstants, ResourceManager}

class ValueIterationResourceManager(constants: RMConstants, streamingContext: StreamingContext) extends ResourceManager(constants, streamingContext) {

}
