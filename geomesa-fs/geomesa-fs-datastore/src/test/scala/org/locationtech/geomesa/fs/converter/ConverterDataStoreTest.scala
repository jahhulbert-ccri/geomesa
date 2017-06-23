/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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
    "work with one datastore" >> {
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path" -> this.getClass.getClassLoader.getResource("example/datastore1").getFile,
        "fs.encoding" -> "converter",
        "fs.options.sft.name" -> "fs-test",   //need to make one
        "fs.options.converter.name" -> "fs-test",
        "fs.partition-scheme.name" -> "date",
        "fs.partition-scheme.opts.datetime-format" -> "yyyy/DDD/HH/mm",
        "fs.partition-scheme.opts.step-unit" -> "MINUTES",
        "fs.partition-scheme.opts.step" -> "15",
        "fs.partition-scheme.opts.partition-attribute" -> "dtg",
        "fs.partition-scheme.opts.leaf-mode" -> "data"
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

    "work with another ds" >> {
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path" -> this.getClass.getClassLoader.getResource("example/datastore2").getFile,
        "fs.encoding" -> "converter",
        "fs.options.sft.name" -> "fs-test",   //need to make one
        "fs.options.converter.name" -> "fs-test",
        "fs.partition-scheme.name" -> "date",
        "fs.partition-scheme.opts.datetime-format" -> "yyyy/DDD/HH/mm",
        "fs.partition-scheme.opts.step-unit" -> "MINUTES",
        "fs.partition-scheme.opts.step" -> "15",
        "fs.partition-scheme.opts.partition-attribute" -> "dtg",
        "fs.partition-scheme.opts.leaf-mode" -> "data"
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
