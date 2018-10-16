package com.sap.rm.rl.impl.statespace

import java.io.{DataInputStream, DataOutputStream}
import java.lang.Math.abs

import com.sap.rm.ResourceManagerConfig
import com.sap.rm.rl.Action.{NoAction, ScaleIn, ScaleOut}
import com.sap.rm.rl.{State, StateActionSet, StateSpace}
import scala.collection.mutable.{HashMap => MutableHashMap}

trait StateSpaceUtils {

  val config: ResourceManagerConfig

  def dangerZoneLatencyDifference(s: State): Double = abs(config.CoarseTargetLatency - s.latency - 1)

  def safeZoneLatencyDifference(s: State): Double = config.CoarseTargetLatency - s.latency

  def isStateInDangerZone(s: State): Boolean = s.latency >= config.CoarseTargetLatency

  def executorRatio(numberOfExecutors: Int): Double = config.MaximumExecutors.toDouble / numberOfExecutors
}

object StateSpaceUtils {
  def writeTo(out: DataOutputStream, stateSpace: StateSpace): Unit = {
    var i: Int = 0

    for ((k: State, v: StateActionSet) <- stateSpace.value.toList.sortWith(_._1 < _._1)) {
      val scaleOutReward: Double = v.qValues(ScaleOut)
      val noActionReward: Double = v.qValues(NoAction)
      val scaleInReward: Double = v.qValues(ScaleIn)

      out.writeInt(k.latency)
      out.writeBoolean(k.loadIsIncreasing)
      out.writeBoolean(v.isVisited)
      out.writeDouble(scaleOutReward)
      out.writeDouble(noActionReward)
      out.writeDouble(scaleInReward)

      i += 1
    }
    out.writeInt(-1000) // mark end of file
    out.writeInt(i) // write number of records as well
  }

  def loadFrom(in: DataInputStream): Option[StateSpace] = {
    var i: Int = 0
    val ss = StateSpace(MutableHashMap[State, StateActionSet]())

    while (true) {
      val latency: Int = in.readInt()
      if (latency == -1000) {
        val numberOfRecords = in.readInt()
        if (i == numberOfRecords) return Some(ss) else None
      }

      val loadIsIncreasing: Boolean = in.readBoolean()
      val isVisited: Boolean = in.readBoolean()
      val scaleOutReward: Double = in.readDouble()
      val noActionReward: Double = in.readDouble()
      val scaleInReward: Double = in.readDouble()

      ss.value += (
        State(latency, loadIsIncreasing) ->
          StateActionSet(
            isVisited = isVisited,
            qValues = MutableHashMap(ScaleOut -> scaleOutReward, NoAction -> noActionReward, ScaleIn -> scaleInReward)
          )
        )

      i += 1
    }

    None
  }
}
