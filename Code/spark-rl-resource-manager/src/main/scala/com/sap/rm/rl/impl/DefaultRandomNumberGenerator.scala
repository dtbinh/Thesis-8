package com.sap.rm.rl.impl

import com.sap.rm.rl.RandomNumberGenerator

class DefaultRandomNumberGenerator(seed: Long = System.currentTimeMillis()) extends RandomNumberGenerator {

  @transient private lazy val rand = new scala.util.Random(seed)

  override def nextDouble(): Double = rand.nextDouble()

  override def nextBoolean(): Boolean = rand.nextBoolean()
}

object DefaultRandomNumberGenerator {
  def apply(seed: Long = System.currentTimeMillis()): DefaultRandomNumberGenerator = new DefaultRandomNumberGenerator(seed)
}