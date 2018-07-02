package com.sap.rl

import org.apache.log4j.BasicConfigurator

object TestCommons {

  // configure log4j
  BasicConfigurator.configure()

  val DynamicResourceAllocationEnabledKey = "spark.streaming.dynamicAllocation.enabled"
  val DynamicResourceAllocationTestingKey = "spark.streaming.dynamicAllocation.testing"
}
