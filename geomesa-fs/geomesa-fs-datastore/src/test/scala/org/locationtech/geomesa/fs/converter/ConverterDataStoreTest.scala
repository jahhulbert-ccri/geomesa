package org.locationtech.geomesa.fs.converter

import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
  * Created by hulbert on 6/21/17.
  */
@RunWith(classOf[JUnitRunner])
class ConverterDataStoreTest extends Specification {
  sequential

  "ConverterDataStore" should {
    "work" >> {
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path" -> this.getClass.getClassLoader.getResource("example/datastore").getFile,
        "fs.encoding" -> "converter",
        "fs.options.sft.name" -> "fs-test",   //need to make one
        "fs.options.converter.name" -> "fs-test"
      ))
      ds must not beNull

      val types = ds.getTypeNames
      types.size mustEqual 1
      types.head mustEqual "fs-test"

      val q = new Query("fs-test", Filter.INCLUDE)
      val fr = ds.getFeatureReader(q, Transaction.AUTO_COMMIT)
      val feats = mutable.ListBuffer.empty[SimpleFeature]
      while (fr.hasNext) {
        feats += fr.next()
      }

      feats.size mustEqual 4


    }
  }
}
