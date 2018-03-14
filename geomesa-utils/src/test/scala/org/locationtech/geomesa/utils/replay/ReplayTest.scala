package org.locationtech.geomesa.utils.replay

import java.text.SimpleDateFormat
import java.time.{Duration, Instant}

import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.iterators.SortingSimpleFeatureIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReplayTest extends Specification {

  sequential

  "Replay" should {
    val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    def features = {
      new SortingSimpleFeatureIterator(
          (0 until 10).map { i =>
            SimpleFeatureBuilder.build(sft, Array[AnyRef](s"$i", df.parse(f"2018-01-01T00:00:$i%02d.000Z")), i.toString)
          }.iterator,
        Seq(("dtg", false))
      )
    }

    "replay things at the same speed" >> {
      val start = Instant.now()
      new ReplayIterator(features, 1, df.parse("2018-01-01T00:00:00.000Z").toInstant, 1.0).foreach(x => {})
      val end = Instant.now()
      val diff = Duration.between(start, end)
      diff.getSeconds must beCloseTo(10L, 1L)
    }

    "replay things 2x" >> {
      val start = Instant.now()
      new ReplayIterator(features, 1, df.parse("2018-01-01T00:00:00.000Z").toInstant, 2.0).foreach(x => {})
      val end = Instant.now()
      val diff = Duration.between(start, end)
      diff.getSeconds must beCloseTo(5L, 1L)
    }

    "replay things 5x" >> {
      val start = Instant.now()
      new ReplayIterator(features, 1, df.parse("2018-01-01T00:00:00.000Z").toInstant, 5.0).foreach(x => {})
      val end = Instant.now()
      val diff = Duration.between(start, end)
      diff.getSeconds must beCloseTo(2L, 1L)
    }
  }

}