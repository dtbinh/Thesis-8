package com.sap.rm

case class Stat (TotalBatches: Int, TotalSLOViolations: Int,
                 AverageProcessingTime: Int, AverageTotalDelay: Int, Executors: Int,
                 WindowID: Int, WindowSize: Int, WindowSLOViolations: Int) {
  override def toString: String = {
    "TotalBatches=%d,TotalSLOViolations=%d,AverageProcessingTime=%d,AverageTotalDelay=%d,Executors=%d,WindowID=%d,WindowSize=%d,WindowSLOViolations=%d"
      .format(TotalBatches,TotalSLOViolations,AverageProcessingTime,AverageTotalDelay,Executors,WindowID,WindowSize,WindowSLOViolations)
  }
}

