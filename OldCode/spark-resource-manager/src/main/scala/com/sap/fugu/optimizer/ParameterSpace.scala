package com.sap.fugu.optimizer

import com.sap.fugu.FuguResourceManager
import org.apache.spark.streaming.scheduler.listener.model.BatchTier

import scala.math.ceil
import scala.math.floor

/**
 * Defines search parameters for ResourceOptimizer. Number of batches is determined by
 * AdaptiveWindow algorithm while threshold is determined by size of that window.
 *
 * @param manager         FuguResourceManager instance
 * @param finishedBatches All the batches registered by manager's listener
 */
class ParameterSpace(manager: FuguResourceManager, finishedBatches: Seq[BatchTier]) {
  val batches: Seq[BatchTier] = {
    val delta          = manager.adaptiveWindowDelta
    val clock          = ceil(0.01 * manager.bufferSize).toInt
    val adaptiveWindow = new AdaptiveWindow(delta, clock)
    val load           = finishedBatches.map(_.records.toDouble)
    val maxLoad        = load.reduceOption(_ max _).getOrElse(0.0)

    load.map(_ / maxLoad).foreach(adaptiveWindow.add)
    finishedBatches.takeRight(adaptiveWindow.getWidth)
  }

  val threshold: Int = {
    val range = manager.thresholdBreaches
    val ratio = batches.size.toDouble / manager.bufferSize

    range.head + floor(ratio * range.size).toInt * range.step
  }
}
