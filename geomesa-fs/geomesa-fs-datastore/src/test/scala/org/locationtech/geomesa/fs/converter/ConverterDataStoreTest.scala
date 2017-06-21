package org.locationtech.geomesa.fs.converter

import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
/**
  * Created by hulbert on 6/21/17.
  */
@RunWith(classOf[JUnitRunner])
class ConverterDataStoreTest extends Specification {
  sequential

  "ConverterDataStore" should {
    "work" >> {
      DataStoreFinder.getDataStore(Map(
        "fs.root" -> this.getClass.getClassLoader.getResource("example/datastore").getFile,
        "fs.encoding" -> "converter",
        "fs.options.sft.name" -> "dstest",   //need to make one
        "fs.options.converter.name" -> "dstest"
      ))
      
    }
  }
}
