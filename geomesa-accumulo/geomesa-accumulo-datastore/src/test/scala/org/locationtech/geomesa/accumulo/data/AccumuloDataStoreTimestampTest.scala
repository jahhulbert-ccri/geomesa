package org.locationtech.geomesa.accumulo.data

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreTimestampTest extends Specification with TestWithMultipleSfts {

  sequential
  override def dsParams = Map(
    "connector" -> connector,
    "caching"   -> false,
    // note the table needs to be different to prevent testing errors
    "tableName" -> sftBaseName)
  val defaultSpec = "name:String,geom:Point:srid=4326,dtg:Date"

  "AccumuloDataStoreTimestampTest" should {
    "create a store" in {
      ds must not(beNull)
    }

    "create a schema" in {
      val defaultSft = createNewSchema(defaultSpec)
      ds.getSchema(defaultSft.getTypeName) mustEqual defaultSft
    }

    "store the DTG as a timestamp" >> {
      val sft = createNewSchema("name:String,dtg:Date,*geom:Point:srid=4326")
      val sftName = sft.getTypeName

      addFeatures(sft, (0 until 6).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.setAttributes(Array[AnyRef](i.toString, "2012-01-02T05:06:07.000Z", "POINT(45.0 45.0)"))
        sf
      })

    }

  }

}
