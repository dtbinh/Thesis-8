package com.sap.rl.rm.vi

import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{RMConstants, ResourceManager}

class ValueIterationResourceManager(constants: RMConstants, streamingContext: StreamingContext)
  extends ResourceManager(constants, streamingContext) {

  import constants._

  @transient private lazy val log = LogManager.getLogger(this.getClass)

  override def specialize(): Unit = {
    super.specialize()


  }
}

object ValueIterationResourceManager {
  def apply(ssc: StreamingContext): ValueIterationResourceManager = {
    val constants: RMConstants = RMConstants(ssc.sparkContext.getConf)
    new ValueIterationResourceManager(constants, ssc)
  }
}
