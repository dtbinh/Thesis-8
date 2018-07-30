package com.sap.rl.rm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext

trait Spark {
  protected def streamingContext: StreamingContext
  protected lazy val sparkContext: SparkContext = streamingContext.sparkContext
  protected lazy val sparkConf: SparkConf = sparkContext.getConf
}
