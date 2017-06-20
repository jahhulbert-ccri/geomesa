package org.locationtech.geomesa.parquet

import org.apache.parquet.filter2.predicate.Operators
import org.geotools.factory.CommonFactoryFinder
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class FilterConverterTest extends Specification with AllExpectations {

  "FilterConverter" should {
    val ff = CommonFactoryFinder.getFilterFactory2
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val conv = new FilterConverter(sft)

    "convert geo filter to min/max x/y" >> {
      val pfilter = conv.convert(ff.bbox("geom", -24.0, -25.0, -18.0, -19.0, "EPSG:4326")).get
      pfilter must beAnInstanceOf[Operators.And]
      // TODO extract the rest of the AND filters to test
      val last = pfilter.asInstanceOf[Operators.And].getRight.asInstanceOf[Operators.LtEq[java.lang.Double]]
      last.getValue mustEqual -19.0
      last.getColumn.getColumnPath.toDotString mustEqual "geom.y"
    }

    "convert dtg ranges to long ranges" >> {
      val pfilter = conv.convert(ff.between(ff.property("dtg"), ff.literal("2017-01-01T00:00:00.000Z"), ff.literal("2017-01-05T00:00:00.000Z"))).get
      pfilter must beAnInstanceOf[Operators.And]
      val and = pfilter.asInstanceOf[Operators.And]
      and.getLeft.asInstanceOf[Operators.GtEq[java.lang.Long]].getColumn.getColumnPath.toDotString mustEqual "dtg"
      and.getLeft.asInstanceOf[Operators.GtEq[java.lang.Long]].getValue mustEqual ISODateTimeFormat.dateTime().parseDateTime("2017-01-01T00:00:00.000Z").getMillis

      and.getRight.asInstanceOf[Operators.LtEq[java.lang.Long]].getColumn.getColumnPath.toDotString mustEqual "dtg"
      and.getRight.asInstanceOf[Operators.LtEq[java.lang.Long]].getValue mustEqual ISODateTimeFormat.dateTime().parseDateTime("2017-01-05T00:00:00.000Z").getMillis
    }
  }
}
