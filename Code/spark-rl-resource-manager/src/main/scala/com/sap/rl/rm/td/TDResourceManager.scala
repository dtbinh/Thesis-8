package com.sap.rl.rm.td

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._

class TDResourceManager(val constants: RMConstants, val streamingContext: StreamingContext) extends ResourceManager(constants, streamingContext) {

  var lastTimeDecisionMade: Long = 0
  var runningSum: Long = 0
  var numberOfBatches: Int = 0

  import constants._

  override val listener: StreamingListener = new BatchListener {

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      val info: BatchInfo = batchCompleted.batchInfo
      val batchTime: Long = info.batchTime.milliseconds

      // log everything
      log.info(s"Received successful batch ${batchTime} ms with completion time")
      log.info(info.toString)

      if (info.totalDelay.isEmpty) {
        log.info(s"Batch ${batchTime} -- IS empty")
      } else {
        log.info(s"Batch ${batchTime} -- ISN'T empty")

        if (batchTime > (streamingStartTime + StartupWaitTime)) {
          log.info(s"Batch ${batchTime} -- AFTER startup phase")

          // if we are not in grace period, check for window size
          if (batchTime > (lastTimeDecisionMade + GracePeriod)) {
            log.info(s"Batch ${batchTime} -- NOT IN grace period")

            synchronized {
              runningSum = runningSum + info.totalDelay.get
              numberOfBatches += 1

              if (numberOfBatches == WindowSize) {
                val avg: Double = runningSum.toDouble / numberOfBatches
                val normalizedAverageLatency: Long = (avg / LatencyGranularity).toLong

                runningSum = 0
                numberOfBatches = 0

                // build the state variable
              }
            }
          } else {
            log.info(s"Batch ${batchTime} -- IN grace period")
          }
        } else {
          log.info(s"Batch ${batchTime} -- BEFORE startup phase")
        }
      }
    }
  }
}

object TDResourceManager {
  def apply(ssc: StreamingContext): TDResourceManager = {
    val constants: RMConstants = RMConstants(ssc.sparkContext.getConf)
    new TDResourceManager(constants, ssc)
  }
}

