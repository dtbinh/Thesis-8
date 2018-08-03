package com.sap.rm

import java.lang.Math.{max, min}

import org.apache.spark.streaming.scheduler.BatchInfo

case class Stat (TotalBatches: Int, TotalSLOViolations: Int,
                 AverageLatency: Int, MinLatency: Int, MaxLatency: Int,
                 AverageExecutors: Int, MinExecutors: Int, MaxExecutors: Int,
                 WindowID: Int, WindowSize: Int, WindowSLOViolations: Int) {
  override def toString: String = {
    "TotalBatches=%d,TotalSLOViolations=%d,AverageLatency=%d[ms],MinLatency=%d[ms],MaxLatency=%d[ms],AverageExecutors=%d,MinExecutors=%d,MaxExecutors=%d,WindowID=%d,WindowSize=%d,WindowSLOViolations=%d"
      .format(TotalBatches,TotalSLOViolations,AverageLatency,MinLatency,MaxLatency,AverageExecutors,MinExecutors,MaxExecutors,WindowID,WindowSize,WindowSLOViolations)
  }
}

class StatBuilder(reportDuration: Long) {

  private var totalSLOViolations: Int = 0
  private var totalBatches: Int = 0

  private var sumLatency: Int = 0
  private var minLatency: Int = Int.MaxValue
  private var maxLatency: Int = Int.MinValue

  private var sumExecutor: Int = 0
  private var minExecutor: Int = Int.MaxValue
  private var maxExecutor: Int = Int.MinValue

  private var windowCounter: Int = 1
  private var windowBatches: Int = 0
  private var windowSLOViolations: Int = 0
  private var lastTimeReportGenerated: Long = 0

  def update(info: BatchInfo, numExecutors: Int, isSLOViolated: Boolean): Option[Stat] = {
    if (lastTimeReportGenerated == 0) {
      lastTimeReportGenerated = info.batchTime.milliseconds
    }

    if ((info.batchTime.milliseconds - lastTimeReportGenerated) >= reportDuration) {
      val stat = Stat(TotalBatches = totalBatches, TotalSLOViolations = totalSLOViolations,
        AverageLatency = sumLatency / windowBatches, MinLatency = minLatency, MaxLatency = maxLatency,
        AverageExecutors = sumExecutor / windowBatches, MinExecutors = minExecutor, MaxExecutors = maxExecutor,
        WindowID = windowCounter, WindowSize = windowBatches, WindowSLOViolations = windowSLOViolations)

      // reset stats
      windowBatches = 0
      windowSLOViolations = 0

      sumLatency = 0
      minLatency = Int.MaxValue
      maxLatency = Int.MinValue

      sumExecutor = 0
      minExecutor = Int.MaxValue
      maxExecutor = Int.MinValue

      lastTimeReportGenerated = info.batchTime.milliseconds
      windowCounter += 1

      return Some(stat)
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

    sumExecutor += numExecutors
    minExecutor = min(minExecutor, numExecutors)
    maxExecutor = max(maxExecutor, numExecutors)

    None
  }
}

object StatBuilder {
  def apply(reportDuration: Long): StatBuilder = new StatBuilder(reportDuration)
}