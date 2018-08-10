package com.sap.rm

import org.apache.spark.streaming.scheduler.BatchInfo

case class Stat (TotalBatches: Int, TotalSLOViolations: Int,
                 AverageProcessingTime: Int, AverageTotalDelay: Int, Executors: Int,
                 WindowID: Int, WindowSize: Int, WindowSLOViolations: Int) {
  override def toString: String = {
    "TotalBatches=%d,TotalSLOViolations=%d,AverageProcessingTime=%d,AverageTotalDelay=%d,Executors=%d,WindowID=%d,WindowSize=%d,WindowSLOViolations=%d"
      .format(TotalBatches,TotalSLOViolations,AverageProcessingTime,AverageTotalDelay,Executors,WindowID,WindowSize,WindowSLOViolations)
  }
}

class StatBuilder(reportDuration: Long) {

  private var totalSLOViolations: Int = 0
  private var totalBatches: Int = 0

  private var sumProcessingTime: Int = 0
  private var sumTotalDelay: Int = 0

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

    sumProcessingTime += info.processingDelay.get.toInt
    sumTotalDelay += info.totalDelay.get.toInt

    if ((info.batchTime.milliseconds - lastTimeReportGenerated) >= reportDuration) {
      val stat = Stat(TotalBatches = totalBatches, TotalSLOViolations = totalSLOViolations,
        AverageProcessingTime = sumProcessingTime / windowBatches, AverageTotalDelay = sumTotalDelay / windowBatches, Executors = numExecutors,
        WindowID = windowCounter, WindowSize = windowBatches, WindowSLOViolations = windowSLOViolations)

      // reset stats
      windowBatches = 0
      windowSLOViolations = 0
      sumProcessingTime = 0
      sumTotalDelay = 0

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