package com.sap.rm

case class Stat (AverageTotalDelay: Int, Executors: Int, WindowID: Int) {
  override def toString: String = {
    "AverageTotalDelay=%d,Executors=%d,WindowID=%d".format(AverageTotalDelay,Executors,WindowID)
  }
}

