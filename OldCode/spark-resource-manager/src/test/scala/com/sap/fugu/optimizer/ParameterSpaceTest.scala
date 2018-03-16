package com.sap.fugu.optimizer

import com.sap.fugu.FuguResourceManager
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.BatchInfo
import org.apache.spark.streaming.scheduler.ResourceManager
import org.apache.spark.streaming.scheduler.listener.model.BatchTier
import org.apache.spark.streaming.utils.ResourceManagerTestUtil
import org.scalatest.FunSuite

class ParameterSpaceTest extends FunSuite with ResourceManagerTestUtil {
  val BatchesNo = 1000

  test("Parameters from sample batch with default config") {
    withStreamingContext(new ResourceManager(_) {})(ssc => {
      val fuguManager = new FuguResourceManager(ssc)
      val space       = new ParameterSpace(fuguManager, generateBatches(BatchesNo, generateLoad))

      assert(space.batches.length < BatchesNo / 2)
      assert(space.threshold > 75 && space.threshold < 125)
    })
  }

  private def generateBatches(n: Int, load: Int => Long): Seq[BatchTier] = {
    (1 to n).map(n => generateBatch(load(n)))
  }

  private def generateLoad(n: Int): Long = if (n < BatchesNo * 2 / 3) 0L else 1L

  private def generateBatch(load: Long): BatchTier = {
    new BatchTier(BatchInfo(Time(0), Map(), 0, Some(load), None, Map()), Nil) {
      override val records: Long = load
    }
  }
}
