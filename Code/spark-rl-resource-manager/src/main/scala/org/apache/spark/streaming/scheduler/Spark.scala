package org.apache.spark.streaming.scheduler

import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.SparkContext.{RDD_SCOPE_KEY => RddScopeKey}

trait Spark {
  def streamingContext: StreamingContext
  lazy val sparkContext: SparkContext = streamingContext.sparkContext
  lazy val sparkConf: SparkConf = sparkContext.getConf
  lazy val batchDuration: Long = streamingContext.graph.batchDuration.milliseconds

  def batchTime(jobStart: SparkListenerJobStart): Long = {
    val pattern       = ".*\"id\":\"[0-9]+_([0-9]+)\".*".r
    val scopeProperty = jobStart.properties.getProperty(RddScopeKey)

    scopeProperty.replaceAll("\\s", "") match {
      case pattern(batchTime) => batchTime.toLong
      case _                  => throw new SparkException(s"Could not extract batch time")
    }
  }
}
