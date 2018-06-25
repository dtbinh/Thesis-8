package org.apache.spark.streaming.scheduler

import org.apache.spark.internal.Logging

trait BatchListener extends StreamingListener with Logging {

  var streamingStartTime: Long = 0

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    streamingStartTime = streamingStarted.time

    log.info(s"streaming was started at ${streamingStartTime}")
  }
}
