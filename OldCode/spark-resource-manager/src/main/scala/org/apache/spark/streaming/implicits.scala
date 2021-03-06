package org.apache.spark.streaming

import org.apache.spark.streaming.scheduler.ResourceManager

/**
 * Package object containing class extension for StreamingContext instantiation with custom
 * ResourceManager
 */
object implicits {
  implicit class StreamingContextWithResourceManager(val ssc: StreamingContext) extends AnyVal {
    def withResourceManager(create: StreamingContext => ResourceManager): StreamingContext =
      new StreamingContextWrapper(ssc, create)
  }
}
