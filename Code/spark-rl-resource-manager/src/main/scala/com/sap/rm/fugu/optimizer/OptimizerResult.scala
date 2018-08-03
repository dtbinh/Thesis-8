package com.sap.rm.fugu.optimizer

case class OptimizerResult(executorNo: Int, windowSize: Int, threshold: Int) {
  override def toString: String = s"$executorNo,$windowSize,$threshold"
}
