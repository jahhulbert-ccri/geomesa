/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Date

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.common.{DateScheme, DateTimeZ2Scheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class PartitionSchemeTest extends Specification with AllExpectations {

  "PartitionScheme" should {
    import scala.collection.JavaConversions._

    val gf = JTSFactoryFinder.getGeometryFactory
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val sf = new SimpleFeatureImpl(
      List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
        gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

    "partition based on date" >> {
      val ps = new DateScheme(DateTimeFormatter.ofPattern("yyyy-MM-dd"), ChronoUnit.DAYS, 1, sft, "dtg")
      ps.getPartitionName(sf) mustEqual "2017-01-03"
    }

    "partition based on date with slash delimiter" >> {
      val ps = new DateScheme(DateTimeFormatter.ofPattern("yyyy/DDD/HH"), ChronoUnit.DAYS, 1, sft, "dtg")
      ps.getPartitionName(sf) mustEqual "2017/003/10"
    }

    "partition based on date with slash delimiter" >> {
      val ps = new DateScheme(DateTimeFormatter.ofPattern("yyyy/DDD/HH"), ChronoUnit.DAYS, 1, sft, "dtg")
      ps.getPartitionName(sf) mustEqual "2017/003/10"
    }

    "10 bit datetime z2 partition" >> {
      val sf = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

      val sf2 = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(-75, 38))), sft, new FeatureIdImpl("1"))

      val ps = new DateTimeZ2Scheme(DateTimeFormatter.ofPattern("yyyy/DDD"), ChronoUnit.DAYS, 1, 10, sft, "dtg", "geom")
      ps.getPartitionName(sf) mustEqual "2017/003/49"
      ps.getPartitionName(sf2) mustEqual "2017/003/12"

    }

    "20 bit datetime z2 partition" >> {
      val sf = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

      val sf2 = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(-75, 38))), sft, new FeatureIdImpl("1"))

      val ps = new DateTimeZ2Scheme(DateTimeFormatter.ofPattern("yyyy/DDD"), ChronoUnit.DAYS, 1, 20, sft, "dtg", "geom")
      ps.getPartitionName(sf) mustEqual "2017/003/196"
      ps.getPartitionName(sf2) mustEqual "2017/003/051"
    }

  }
}
