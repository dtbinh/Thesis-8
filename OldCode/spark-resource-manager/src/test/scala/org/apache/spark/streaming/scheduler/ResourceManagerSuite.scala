package org.apache.spark.streaming.scheduler

import org.apache.spark.SparkDriverExecutionException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.ResourceManager._
import org.apache.spark.streaming.utils.ResourceManagerTestUtil
import org.scalatest.FunSuite

class ResourceManagerSuite extends FunSuite with ResourceManagerTestUtil {
  test("Break initialization with improper executor number bounds") {
    val conf = getSparkConf.set(MinimumExecutorsKey, "2").set(MaximumExecutorsKey, "1")

    withStreamingContext(createDummyResourceManager, conf) { streamingContext =>
      assertThrows[SparkDriverExecutionException] {
        streamingContext.start()
      }
    }
  }

  private def createDummyResourceManager(ssc: StreamingContext): ResourceManager =
    new ResourceManager(ssc) {}
}
