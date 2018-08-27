package com.sap.rm.rl.impl

import com.sap.rm.rl.RandomNumberGenerator

class DefaultRandomNumberGenerator extends RandomNumberGenerator {

  @transient private lazy val rand = new scala.util.Random(System.currentTimeMillis())

  override def nextDouble(): Double = rand.nextDouble()

  override def nextBoolean(): Boolean = rand.nextBoolean()
}
