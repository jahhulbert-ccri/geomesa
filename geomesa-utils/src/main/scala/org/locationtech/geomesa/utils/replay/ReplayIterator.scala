package org.locationtech.geomesa.utils.replay

import java.time.Instant
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.opengis.feature.simple.SimpleFeature
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.locationtech.geomesa.utils.iterators.SortingSimpleFeatureIterator

class ReplayIterator(sortedFeatureIterator: SortingSimpleFeatureIterator,
                     dateIndex: Int,
                     dataStartTime: Instant,
                     rate: Double) extends Iterator[SimpleFeature] with LazyLogging {

  logger.info(s"Starting replay at ${Instant.now()} at rate $rate")
  private val clock = ReplayClock(dataStartTime, rate)
  clock.start()

  private var queued: SimpleFeature = _

  override def hasNext(): Boolean = queued != null || queueNext()

  override def next(): SimpleFeature = {
    hasNext()
    val temp = queued
    queued = null
    temp
  }

  private def queueNext(): Boolean = {
    if (sortedFeatureIterator.hasNext) {
      val temp = sortedFeatureIterator.next()
      val nextT = temp.get[Date](dateIndex).toInstant
      if (nextT.isAfter(clock.replayTime)) {
        clock.sleepUtil(nextT)
      }
      queued = temp
      true
    } else {
      false
    }
  }
}