package com.sap.rm

case class Stat (TotalBatches: Int, AverageTotalDelay: Int, Executors: Int, WindowID: Int, WindowSize: Int) {
  override def toString: String = {
    "TotalBatches=%d,AverageTotalDelay=%d,Executors=%d,WindowID=%d,WindowSize=%d".format(TotalBatches,AverageTotalDelay,Executors,WindowID,WindowSize)
  }
}

