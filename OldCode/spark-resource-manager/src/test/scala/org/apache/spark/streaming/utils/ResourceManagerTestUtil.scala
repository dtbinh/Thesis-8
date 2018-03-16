package org.apache.spark.streaming.utils

import com.sap.fugu.FuguResourceManager._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.ResourceManager
import org.apache.spark.streaming.scheduler.ResourceManager.MinimumExecutorsKey
import org.mockito.Mockito.spy
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar

trait ResourceManagerTestUtil extends MockitoSugar {
  import ResourceManagerTestUtil._

  private val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
    .set(DynamicResourceAllocationEnabledKey, "true")
    .set(DynamicResourceAllocationTestingKey, "true")
    .set(CoresPerTaskKey, "1")
    .set(CoresPerExecutorKey, "1")
    .set(MinimumExecutorsKey, "2")
  val DurationPeriod: Duration = Milliseconds(500)

  def getSparkConf: SparkConf = sparkConf.clone()

  def withStreamingContext(
      create: StreamingContext => ResourceManager,
      conf: SparkConf = getSparkConf
  )(body: StreamingContextWrapper => Unit): Unit = ResourceManagerTestUtil.synchronized {
    var ssc: Option[StreamingContextWrapper] = None
    try {
      ssc = Some(new StreamingContextWrapper(createDummyStreamingContext(conf), create))
      new DummyInputDStream(ssc.get).foreachRDD(_ => {})
      body(ssc.get)
    } finally {
      ssc.foreach(_.stop(stopSparkContext = true, stopGracefully = false))
    }
  }

  def createDummySparkContext(conf: SparkConf): SparkContext = {
    val sc: SparkContext = spy(new SparkContext(conf.clone()))
    when(sc.schedulerBackend) thenReturn mock[CoarseGrainedSchedulerBackend]
    sc.setLogLevel("WARN")
    sc
  }

  def createDummyStreamingContext(conf: SparkConf): StreamingContext = {
    new StreamingContext(createDummySparkContext(conf), DurationPeriod)
  }

  private class DummyInputDStream(context: StreamingContext) extends InputDStream[Int](context) {
    override def start(): Unit                         = {}
    override def stop(): Unit                          = {}
    override def compute(time: Time): Option[RDD[Int]] = Some(context.sc.emptyRDD[Int])
  }
}

object ResourceManagerTestUtil {
  val DynamicResourceAllocationEnabledKey = "spark.streaming.dynamicAllocation.enabled"
  val DynamicResourceAllocationTestingKey = "spark.streaming.dynamicAllocation.testing"
}
