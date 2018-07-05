package com.sap.rl.rm.vi

import com.sap.rl.rm.{RMConstants, ResourceManager}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.StreamingContext

class ValueIterationResourceManager(val constants: RMConstants, val streamingContext: StreamingContext) extends ResourceManager {

  @transient override lazy val log: Logger = LogManager.getLogger(this.getClass)

  override def specialize(): Unit = {
    super.specialize()
  }
}

object ValueIterationResourceManager {
  def apply(constants: RMConstants, ssc: StreamingContext): ValueIterationResourceManager = {
    new ValueIterationResourceManager(constants, ssc)
  }
}
