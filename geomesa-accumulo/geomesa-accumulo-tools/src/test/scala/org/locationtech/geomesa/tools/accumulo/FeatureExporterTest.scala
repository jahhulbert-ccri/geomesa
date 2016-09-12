/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, StringWriter}
import java.util.Date
import java.util.zip.Deflater

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureStore
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.tools.accumulo.Utils.Formats
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FeatureExporterTest extends Specification {

  sequential

  def getDataStoreAndSft(sftName: String, numFeatures: Int = 1) = {
    val sft = SimpleFeatureTypes.createType(sftName, "name:String,geom:Point:srid=4326,dtg:Date")

    val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)

    val attributes = Array("myname", "POINT(45.0 49.0)", new Date(0))
    (1 to numFeatures).foreach { i =>
      val feature = ScalaSimpleFeatureFactory.buildFeature(sft, attributes, s"fid-$i")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      featureCollection.add(feature)
    }

    val connector = new MockInstance().getConnector("", new PasswordToken(""))

    val ds = DataStoreFinder
      .getDataStore(Map("connector" -> connector, "tableName" -> sftName, "caching"   -> false))
    ds.createSchema(sft)
    ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore].addFeatures(featureCollection)
    (ds, sft)
  }

  "DelimitedExport" >> {
    val sftName = "DelimitedExportTest"
    val (ds, sft) = getDataStoreAndSft(sftName)

    "should properly export to CSV" >> {
      val query = new Query(sftName, Filter.INCLUDE)
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV)
      export.write(features)
      export.close()

      val result = writer.toString.split("\r\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "id,name:String,*geom:Point:srid=4326,dtg:Date"
      data mustEqual "fid-1,myname,POINT (45 49),1970-01-01T00:00:00.000Z"
    }

    "should handle projections" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV)
      export.write(features)
      export.close()

      val result = writer.toString.split("\r\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "id,*geom:Point:srid=4326,dtg:Date"
      data mustEqual "fid-1,POINT (45 49),1970-01-01T00:00:00.000Z"
    }

    "should handle transforms" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("derived=strConcat(name, '-test')", "geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV)
      export.write(features)
      export.close()

      val result = writer.toString.split("\r\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "id,*geom:Point:srid=4326,dtg:Date,derived:String"
      data mustEqual "fid-1,POINT (45 49),1970-01-01T00:00:00.000Z,myname-test"
    }

    "should handle escapes" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("derived=strConcat(name, ',test')", "geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV)
      export.write(features)
      export.close()

      val result = writer.toString.split("\r\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "id,*geom:Point:srid=4326,dtg:Date,derived:String"
      data mustEqual "fid-1,POINT (45 49),1970-01-01T00:00:00.000Z,\"myname,test\""
    }
  }

  "Shapefile Export" >> {
    val sftName = "ShapefileExportTest"
    val (ds, sft) = getDataStoreAndSft(sftName)

    def checkReplacedAttributes(attrString: String, expectedString: String) = {
      ShapefileExport.replaceGeomInAttributesString(attrString, sft) mustEqual expectedString
    }

    "should transform 'geom' to 'the_geom' when asking for just 'geom'" >> {
      checkReplacedAttributes("geom", "the_geom=geom")
    }

    "should transform 'geom' in the attributes string when another attribute follows" >> {
      checkReplacedAttributes("geom, name", "the_geom=geom,name")
    }


    "should transform 'geom' in the attributes string when it follows another attribute" >> {
      checkReplacedAttributes("name, geom", "name,the_geom=geom")
    }

    "should transform 'geom' in the attributes string when it is between two attributes" >> {
      checkReplacedAttributes("name, geom, dtg", "name,the_geom=geom,dtg")
    }

    "should transform 'geom' in the attributes string when it is between two attributes without spaces" >> {
      checkReplacedAttributes("name,geom,dtg", "name,the_geom=geom,dtg")
    }

    "should NOT transform 'the_geom' in the attributes string" >> {
      checkReplacedAttributes("the_geom", "the_geom")
    }

    "should NOT transform and incorrect transform in the query" >> {
      checkReplacedAttributes("name,geom=the_geom,dtg", "name,geom=the_geom,dtg")
    }
  }

  "Avro Export" >> {
    val sftName = "AvroExportTest"
    val (ds, sft) = getDataStoreAndSft(sftName, 10)

    "should properly export to avro" >> {
      val query = new Query(sftName, Filter.INCLUDE)
      val featureCollection = ds.getFeatureSource(sftName).getFeatures(query)

      val os = new ByteArrayOutputStream()
      val export = new AvroExport(os, sft, Deflater.NO_COMPRESSION)
      export.write(featureCollection)
      export.close()

      val result = new AvroDataFileReader(new ByteArrayInputStream(os.toByteArray))
      SimpleFeatureTypes.encodeType(result.getSft) mustEqual SimpleFeatureTypes.encodeType(sft)

      val features = result.toList
      features must haveLength(10)
      features.map(_.getID) must containTheSameElementsAs((1 to 10).map("fid-" + _))
      forall(features) { feature =>
        feature.getAttribute(0) mustEqual "myname"
        feature.getAttribute(1) mustEqual WKTUtils.read("POINT(45 49)")
        feature.getAttribute(2) mustEqual new Date(0)
      }
    }

    "should compress output" >> {
      val query = new Query(sftName, Filter.INCLUDE)
      val featureCollection = ds.getFeatureSource(sftName).getFeatures(query)

      val uncompressed :: compressed :: Nil = List(Deflater.NO_COMPRESSION, Deflater.DEFAULT_COMPRESSION).map { c =>
        val os = new ByteArrayOutputStream()
        val export = new AvroExport(os, sft, c)
        export.write(featureCollection)
        export.close()
        os.toByteArray
      }

      forall(Seq(uncompressed, compressed)) { bytes =>
        val result = new AvroDataFileReader(new ByteArrayInputStream(bytes))
        SimpleFeatureTypes.encodeType(result.getSft) mustEqual SimpleFeatureTypes.encodeType(sft)

        val features = result.toList
        features must haveLength(10)
        features.map(_.getID) must containTheSameElementsAs((1 to 10).map("fid-" + _))
        forall(features) { feature =>
          feature.getAttribute(0) mustEqual "myname"
          feature.getAttribute(1) mustEqual WKTUtils.read("POINT(45 49)")
          feature.getAttribute(2) mustEqual new Date(0)
        }
      }

      compressed.length must beLessThan(uncompressed.length)
    }

    "should handle transforms" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("derived=strConcat(name, '-test')", "geom", "dtg"))
      val featureCollection = ds.getFeatureSource(sftName).getFeatures(query)

      val os = new ByteArrayOutputStream()
      val export = new AvroExport(os, featureCollection.getSchema, Deflater.NO_COMPRESSION)
      export.write(featureCollection)
      export.close()

      val result = new AvroDataFileReader(new ByteArrayInputStream(os.toByteArray))
      SimpleFeatureTypes.encodeType(result.getSft) mustEqual SimpleFeatureTypes.encodeType(featureCollection.getSchema)

      val features = result.toList
      features must haveLength(10)
      features.map(_.getID) must containTheSameElementsAs((1 to 10).map("fid-" + _))
      forall(features) { feature =>
        features.head.getAttribute(0) mustEqual WKTUtils.read("POINT(45 49)")
        features.head.getAttribute(1) mustEqual new Date(0)
        features.head.getAttribute(2) mustEqual "myname-test" // derived variable gets bumped to the end
      }
    }
  }
}
