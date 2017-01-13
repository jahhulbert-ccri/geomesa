package org.locationtech.geomesa.features.avro

import java.io.FileOutputStream

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
@RunWith(classOf[JUnitRunner])
class GenericDataTest  extends Specification with AbstractAvroSimpleFeatureTest {
  sequential //because of file delete step

  "AvroDataFile" should {
    "be readable by generic reader" >> {
      val sf = createSimpleFeature
      sf.getUserData.put("key1", "123")
      sf.getUserData.put("key2", "456")
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID("fid1")

      val bad = sf.getUserData.find(_._1.toString.startsWith("USE")).map(_._1)
      sf.getUserData.remove(bad.get)

      val tmpFile = getTmpFile
      val dfw = new AvroDataFileWriter(new FileOutputStream(tmpFile), simpleSft)
      try {
        dfw.append(sf)
      } finally {
        dfw.close()
      }

      val datumReader = new GenericDatumReader[GenericRecord]()
      val dataFileReader = new DataFileReader[GenericRecord](tmpFile, datumReader)
      val meta = dataFileReader.getMetaKeys
      val schema = dataFileReader.getSchema
      datumReader.setSchema(schema)

      while(dataFileReader.hasNext) {
        val genericRecord = dataFileReader.next()
        println(genericRecord)
      }
      success
    }
  }

  step {
    filesCreated.foreach(_.delete)
  }
}
