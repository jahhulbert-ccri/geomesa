/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet


import java.nio.file.Files

import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ParquetReadWriteTest extends Specification with AllExpectations {

  sequential

  "SimpleFeatureParquetWriter" should {

    val f = Files.createTempFile("geomesa", ".parquet")
    val gf = JTSFactoryFinder.getGeometryFactory
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

    "write parquet files" >> {
      val writer = new SimpleFeatureParquetWriter(new Path(f.toUri), new SimpleFeatureWriteSupport(sft))

      val sf = new ScalaSimpleFeature("1", sft, Array("first", Integer.valueOf(100), new java.util.Date, gf.createPoint(new Coordinate(25.236263, 27.436734))))
      val sf2 = new ScalaSimpleFeature("2", sft, Array(null, Integer.valueOf(200), new java.util.Date, gf.createPoint(new Coordinate(67.2363, 55.236))))
      val sf3 = new ScalaSimpleFeature("3", sft, Array("third", Integer.valueOf(300), new java.util.Date, gf.createPoint(new Coordinate(73.0, 73.0))))
      writer.write(sf)
      writer.write(sf2)
      writer.write(sf3)
      writer.close()
      Files.size(f) must be greaterThan 0
    }

    "read parquet files" >> {
      val reader = ParquetReader.builder[SimpleFeature](new SimpleFeatureReadSupport(sft), new Path(f.toUri))
        .withFilter(FilterCompat.NOOP)
        .build()

      val sf = reader.read()
      sf.getAttributeCount mustEqual 4
      sf.getID must be equalTo "1"
      sf.getAttribute("name") must be equalTo "first"
      sf.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 25.236263
      sf.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 27.436734

      val sf2 = reader.read()
      sf2.getAttributeCount mustEqual 4
      sf2.getID must be equalTo "2"
      sf2.getAttribute("name") must beNull
      sf2.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 67.2363
      sf2.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 55.236

      val sf3 = reader.read()
      sf3.getAttributeCount mustEqual 4
      sf3.getID must be equalTo "3"
      sf3.getAttribute("name") must be equalTo "third"
      sf3.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0
      sf3.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 73.0
    }

    "only read transform columns" >> {
      val tsft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      val reader = ParquetReader.builder[SimpleFeature](new SimpleFeatureReadSupport(tsft), new Path(f.toUri))
        .withFilter(FilterCompat.NOOP)
        .build()
      val sf = reader.read()
      sf.getAttributeCount mustEqual 3
      sf.getID must be equalTo "1"
      sf.getAttribute("name") must be equalTo "first"
      sf.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 25.236263
      sf.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 27.436734

      val sf2 = reader.read()
      sf2.getAttributeCount mustEqual 3
      sf2.getID must be equalTo "2"
      sf2.getAttribute("name") must beNull
      sf2.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 67.2363
      sf2.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 55.236

    }

    "perform filtering" >> {
      val tsft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")

      val ff = CommonFactoryFinder.getFilterFactory2
      val geoFilter = ff.equals(ff.property("name"), ff.literal("first"))

      def getFeatures(geoFilter: org.opengis.filter.Filter): Seq[SimpleFeature] = {
        val pFilter = FilterCompat.get(new FilterConverter(tsft).toParquet(geoFilter).get)

        val reader = ParquetReader.builder[SimpleFeature](new SimpleFeatureReadSupport(tsft), new Path(f.toUri))
          .withFilter(pFilter)
          .build()

        val res = mutable.ListBuffer.empty[SimpleFeature]
        var sf: SimpleFeature = reader.read()
        while( sf != null) {
          res += sf
          sf = reader.read()
        }
        res
      }

      "equals" >> {
        val res = getFeatures(ff.equals(ff.property("name"), ff.literal("first")))
        res.size mustEqual 1
        val sf = res.head

        sf.getAttributeCount mustEqual 2
        sf.getID must be equalTo "1"
        sf.getAttribute("name") must be equalTo "first"
        sf.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 25.236263
        sf.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 27.436734

      }

      "not equals" >> {
        val res = getFeatures(ff.notEqual(ff.property("name"), ff.literal("first")))
        res.size mustEqual 2

        val two = res.head
        two.getAttributeCount mustEqual 2
        two.getID must be equalTo "2"
        two.getAttribute("name") must beNull
        two.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 67.2363
        two.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 67.2363

        val three = res.last
        three.getAttributeCount mustEqual 2
        three.getID must be equalTo "3"
        three.getAttribute("name") must be equalTo "third"
        three.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0
        three.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0
      }

    }

    "query with a bbox" >> {

    }

    step {
       Files.deleteIfExists(f)
    }

  }
}
