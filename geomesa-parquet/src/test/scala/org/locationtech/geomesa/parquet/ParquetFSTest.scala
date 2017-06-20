/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ParquetFSTest extends Specification with AllExpectations {

  sequential

  "ParquetFileSystemStorage" should {

    val gf = JTSFactoryFinder.getGeometryFactory
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val ff = CommonFactoryFinder.getFilterFactory2

    "create an fs" >> {
      val parquetFactory = new ParquetFileSystemStorageFactory

      val fsStorage = parquetFactory.build(Map(
        "fs.path" -> "/tmp/andrew"
      ))

      fsStorage.createNewFeatureType(sft)

      fsStorage.listFeatureTypes().size mustEqual 1
      fsStorage.listFeatureTypes().head.getTypeName mustEqual "test"

      val writer = fsStorage.getWriter("test", "1")
      val sf = new ScalaSimpleFeature("1", sft, Array("first", Integer.valueOf(100), new java.util.Date, gf.createPoint(new Coordinate(25.236263, 27.436734))))
      val sf2 = new ScalaSimpleFeature("2", sft, Array(null, Integer.valueOf(200), new java.util.Date, gf.createPoint(new Coordinate(67.2363, 55.236))))
      val sf3 = new ScalaSimpleFeature("3", sft, Array("third", Integer.valueOf(300), new java.util.Date, gf.createPoint(new Coordinate(73.0, 73.0))))
      writer.write(sf)
      writer.write(sf2)
      writer.write(sf3)
      writer.close()
      writer.close()

      val res3 = fsStorage.getReader(new Query("test", ff.equals(ff.property("name"), ff.literal("third"))), "1").toList
      res3.size mustEqual 1
      res3.head.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0

      val res1 = fsStorage.getReader(new Query("test", ff.equals(ff.property("name"), ff.literal("first"))), "1").toList
      res1.size mustEqual 1
      res1.head.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 25.236263

    }

    step {
//      FileUtils.deleteDirectory(new File("/tmp/andrew"))
    }

  }
}
