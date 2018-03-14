package org.locationtech.geomesa.utils.replay

import java.time.{Duration, Instant}

import com.typesafe.scalalogging.LazyLogging

case class ReplayClock(dataStartTime: Instant, rate: Double = 1.0) extends LazyLogging {

  private def now = Instant.now()
  private var realTime: Instant = _

  def start(): Unit = realTime = now

  def replayTime: Instant = {
    val diff = (math.abs(Duration.between(now, realTime).toMillis) * rate).toLong
    dataStartTime.plusMillis(diff)
  }

  def sleepUtil(simTime: Instant) {
    val fakeTime = replayTime
    if (simTime.isAfter(fakeTime)) {
      val sleep = math.abs(Duration.between(simTime, fakeTime).toMillis)
      logger.debug(s"Sleeping for $sleep millis")
      Thread.sleep(sleep)
    }
  }
}
