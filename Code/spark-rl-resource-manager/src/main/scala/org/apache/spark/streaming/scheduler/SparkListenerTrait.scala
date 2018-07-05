package org.apache.spark.streaming.scheduler

import org.apache.spark.scheduler._

// Since SparkListener is not a trait, it can't be mixed in to Resourcemanager
trait SparkListenerTrait extends SparkListenerInterface {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {}

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {}

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {}

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {}

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {}

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {}

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}

  override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {}

  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {}

  override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {}

  override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = {}

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {}

  override def onOtherEvent(event: SparkListenerEvent): Unit = {}
}
