package com.sap.rm

import org.apache.spark.streaming.scheduler.BatchInfo

class StatBuilder(reportDuration: Long) {

  private var totalBatches: Int = 0
  private var sumTotalDelay: Int = 0

  private var windowCounter: Int = 1
  private var windowBatches: Int = 0
  private var lastTimeReportGenerated: Long = 0

  def update(info: BatchInfo, numExecutors: Int): Option[Stat] = {
    if (lastTimeReportGenerated == 0) {
      lastTimeReportGenerated = info.batchTime.milliseconds
    }

    totalBatches += 1
    windowBatches += 1

    sumTotalDelay += info.totalDelay.get.toInt

    if ((info.batchTime.milliseconds - lastTimeReportGenerated) >= reportDuration) {
      val stat = Stat(TotalBatches = totalBatches, AverageTotalDelay = sumTotalDelay / windowBatches, Executors = numExecutors, WindowID = windowCounter, WindowSize = windowBatches)

      // reset stats
      windowBatches = 0
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
