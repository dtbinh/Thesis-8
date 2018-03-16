package org.apache.spark.streaming

import org.apache.spark.SparkDriverExecutionException
import org.apache.spark.streaming.scheduler.ResourceManager
import org.apache.spark.streaming.utils.ResourceManagerTestUtil
import org.apache.spark.streaming.utils.ResourceManagerTestUtil.DynamicResourceAllocationEnabledKey
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

class StreamingContextWrapperSuite
    extends FunSuite with ResourceManagerTestUtil with BeforeAndAfter {
  var flip: Boolean = false

  before {
    flip = false
  }

  test("Start without dynamicAllocation flag") {
    withStreamingContext(createDummyResourceManager) { streamingContext =>
      streamingContext.conf.set(DynamicResourceAllocationEnabledKey, "false")

      assertThrows[SparkDriverExecutionException] {
        streamingContext.start()
      }
    }
  }

  test("Start and stop with dynamicAllocation flag") {
    withStreamingContext(createDummyResourceManager) { streamingContext =>
      streamingContext.start()
      assert(flip)
      streamingContext.stop()
      assert(!flip)
    }
  }

  def createDummyResourceManager(ssc: StreamingContext): ResourceManager = {
    new ResourceManager(ssc) {
      override def start(): Unit = flip = true
      override def stop(): Unit  = flip = false
    }
  }
}
