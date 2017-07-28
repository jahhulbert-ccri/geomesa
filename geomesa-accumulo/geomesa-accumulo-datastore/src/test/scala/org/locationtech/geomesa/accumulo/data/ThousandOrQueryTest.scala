/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypes, SftBuilder}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ThousandOrQueryTest extends Specification with TestWithDataStore {

  val spec = "attr:String,dtg:Date,*geom:Point:srid=4326"
  Random.setSeed(0L)

  val numFeatures = 5000
  val features = (0 until numFeatures).map { i =>
    val a = (0 until 20).map(i => Random.nextInt(9).toString).mkString + "<foobar>"
    val values = Array[AnyRef](a, f"2014-01-01T01:00:$i%02d.000Z", WKTUtils.read(s"POINT(45.0 45.$i)") )
    val sf = new ScalaSimpleFeature(i.toString, sft, values)
    sf.getUserData.update(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  addFeatures(features)

  "AccumuloDataStore" should {
    "not take forever to plan a crazy query" >> {
      val filt = ECQL.toFilter("attr in (" + features.take(1200).map(_.getAttribute(0)).map(a => s"'$a'").mkString(", ") + ")")
      val start = System.currentTimeMillis()
      fs.getFeatures(filt)
      val end = System.currentTimeMillis()
      println(end - start)
      success
    }
  }

}
