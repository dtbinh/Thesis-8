package org.apache.spark.streaming.scheduler

import com.sap.rl.rm.LogStatus.STREAMING_STARTED
import org.apache.log4j.Logger

trait SparkStreamingListenerTrait extends StreamingListener {

  protected val log: Logger

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    log.info(s"$STREAMING_STARTED -- StreamingStartTime = ${streamingStarted.time}")
  }
}
