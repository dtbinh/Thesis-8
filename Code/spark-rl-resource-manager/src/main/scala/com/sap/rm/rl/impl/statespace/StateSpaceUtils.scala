package com.sap.rm.rl.impl.statespace

import java.lang.Math.abs

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.State

trait StateSpaceUtils {

  val config: ResourceManagerConfig

  def dangerZoneLatencyDifference(s: State): Double = abs(config.CoarseTargetLatency - s.latency - 1)

  def safeZoneLatencyDifference(s: State): Double = config.CoarseTargetLatency - s.latency

  def isStateInDangerZone(s: State): Boolean = s.latency >= config.CoarseTargetLatency

  def executorRatio(s: State): Double = config.MaximumExecutors.toDouble / s.numberOfExecutors
}
