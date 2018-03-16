package com.sap.fugu.optimizer

import com.sap.fugu.FuguResourceManager
import com.sap.fugu.simulation.BatchSimulator
import org.apache.spark.streaming.scheduler.listener.model.BatchTier

import scala.annotation.tailrec
import scala.math.max

/**
 * Determines optimal number of working executors that promises to preserve latency constraint
 * while lowering resource usage costs. It utilizes simple binary-search-like algorithm for
 * calculating result.
 *
 * This is done with assumption that cost function is non-increasing up to certain point - after
 * which it's increasing. Values for those two parts are delimited by Int.MaxValue (which is equal
 * to 2**31-1).
 *
 * The aim of the cost function is to find the lowest number of executors not breaching the
 * threshold constraint with calculated delays. Time od performing each batch with fixed number
 * of executors is determined by BatchSimulator.
 *
 * @param manager FuguResourceManager instance
 */
class ResourceOptimizer(manager: FuguResourceManager) {
  def calculateExecutors(batches: Seq[BatchTier]): OptimizerResult = {
    val receiverNo = manager.receivers.size
    val lowerBound = max(manager.minimumExecutors - receiverNo, 1)
    val upperBound = max(manager.maximumExecutors - receiverNo, lowerBound)
    val parameters = new ParameterSpace(manager, batches)
    val executorNo = receiverNo + search(lowerBound, upperBound, parameters)

    OptimizerResult(executorNo, parameters.batches.size, parameters.threshold)
  }

  @tailrec
  private def search(begin: Int, end: Int, parameters: ParameterSpace): Int = {
    if (begin < end) {
      val middle = (begin + end) / 2
      if (calculateCost(middle, parameters) > Int.MaxValue) {
        search(middle + 1, end, parameters)
      } else {
        search(begin, middle, parameters)
      }
    } else {
      end
    }
  }

  private def calculateCost(executors: Int, parameters: ParameterSpace): Long = {
    val simulatedTimes  = parameters.batches.map(new BatchSimulator(_, executors).simulate())
    val simulatedDelays = calculateDelays(simulatedTimes)
    val breaches        = simulatedDelays.count(_ > 0L)

    if (breaches > parameters.threshold) {
      Int.MaxValue.toLong + breaches
    } else {
      executors
    }
  }

  private def calculateDelays(times: Seq[Long]): Seq[Long] = {
    (1 to times.size) map { n =>
      times.take(n).foldLeft(0L)((acc, t) => max(t - manager.batchDuration + acc, 0L))
    }
  }
}
