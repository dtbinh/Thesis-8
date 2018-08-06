package com.sap.rm

import java.lang.Math.{max, min}

import org.apache.spark.streaming.scheduler.BatchInfo

case class Stat (TotalBatches: Int, TotalSLOViolations: Int,
                 AverageLatency: Int, MinLatency: Int, MaxLatency: Int,
                 Executors: Int, WindowID: Int, WindowSize: Int, WindowSLOViolations: Int) {
  override def toString: String = {
    "TotalBatches=%d,TotalSLOViolations=%d,AverageLatency=%d,MinLatency=%d,MaxLatency=%d,Executors=%d,WindowID=%d,WindowSize=%d,WindowSLOViolations=%d"
      .format(TotalBatches,TotalSLOViolations,AverageLatency,MinLatency,MaxLatency,Executors,WindowID,WindowSize,WindowSLOViolations)
  }
}

class StatBuilder(reportDuration: Long) {

  private var totalSLOViolations: Int = 0
  private var totalBatches: Int = 0

  private var sumLatency: Int = 0
  private var minLatency: Int = Int.MaxValue
  private var maxLatency: Int = Int.MinValue

  private var windowCounter: Int = 1
  private var windowBatches: Int = 0
  private var windowSLOViolations: Int = 0
  private var lastTimeReportGenerated: Long = 0

  def update(info: BatchInfo, numExecutors: Int, isSLOViolated: Boolean): Option[Stat] = {
    if (lastTimeReportGenerated == 0) {
      lastTimeReportGenerated = info.batchTime.milliseconds
    }

    totalBatches += 1
    windowBatches += 1
    if (isSLOViolated) {
      totalSLOViolations += 1
      windowSLOViolations += 1
    }

    sumLatency += info.processingDelay.get.toInt
    minLatency = min(minLatency, info.processingDelay.get.toInt)
    maxLatency = max(maxLatency, info.processingDelay.get.toInt)

    if ((info.batchTime.milliseconds - lastTimeReportGenerated) >= reportDuration) {
      val stat = Stat(TotalBatches = totalBatches, TotalSLOViolations = totalSLOViolations,
        AverageLatency = sumLatency / windowBatches, MinLatency = minLatency, MaxLatency = maxLatency,
        Executors = numExecutors, WindowID = windowCounter, WindowSize = windowBatches, WindowSLOViolations = windowSLOViolations)

      // reset stats
      windowBatches = 0
      windowSLOViolations = 0

      sumLatency = 0
      minLatency = Int.MaxValue
      maxLatency = Int.MinValue

      lastTimeReportGenerated = info.batchTime.milliseconds
      windowCounter += 1

      return Some(stat)
    }

    None
  }
}

object StatBuilder {
  def apply(reportDuration: Long): StatBuilder = new StatBuilder(reportDuration)
}