package org.locationtech.geomesa.accumulo.data

import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SmallBBOXTest extends Specification with TestWithMultipleSfts  {

  sequential

  val ff = CommonFactoryFinder.getFilterFactory2
  val defaultSft = createNewSchema("name:String:index=join,geom:Point:srid=4326,dtg:Date")

  addFeature(defaultSft, ScalaSimpleFeature.create(defaultSft, "fid-1", "name1", "POINT(104.19004 39.529658)", "2010-05-07T04:30:00.000Z"))
  addFeature(defaultSft, ScalaSimpleFeature.create(defaultSft, "fid-2", "name2", "POINT(104.190169 39.529619)", "2010-05-07T08:30:00.000Z"))
  addFeature(defaultSft, ScalaSimpleFeature.create(defaultSft, "fid-3", "name3", "POINT(104.189722 39.529467)", "2010-05-07T12:30:00.000Z"))
  addFeature(defaultSft, ScalaSimpleFeature.create(defaultSft, "fid-4", "name4", "POINT(104.190011 39.529425)", "2010-05-07T14:30:00.000Z"))
  addFeature(defaultSft, ScalaSimpleFeature.create(defaultSft, "fid-5", "name5", "POINT(104.189797 39.529322)", "2010-05-07T16:30:00.000Z"))
  addFeature(defaultSft, ScalaSimpleFeature.create(defaultSft, "fid-6", "name6", "POINT(104.189836 39.529164)", "2010-05-07T18:30:00.000Z"))
  addFeature(defaultSft, ScalaSimpleFeature.create(defaultSft, "fid-7", "name7", "POINT(104.190153 39.528925)", "2010-05-07T20:30:00.000Z"))

  "Geomesa" should {
    "query with small bboxes" >> {
      val fs = ds.getFeatureSource(defaultSft.getTypeName)

      // bottom left,
      val cqlFilter = ECQL.toFilter(s"dtg between 2010-05-07T00:00:00.000Z and 2010-05-08T00:00:00.000Z and within(geom, POLYGON((104.18975079390569 39.52894876216782, " +
        s"                                                      104.18975079390569 39.52962397496765, " +
        s"                                                      104.19029288401669 39.52962397496765, " +
        s"                                                      104.19029288401669 39.52894876216782, " +
        s"                                                      104.18975079390569 39.52894876216782)))")


      val query = new Query(defaultSft.getTypeName, cqlFilter)


      import org.locationtech.geomesa.utils.geotools.Conversions._
      val results = fs.getFeatures(query)
      val features = results.features.toSeq


      println(features)
      features.size mustEqual 4
    }
  }
}
