/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.geotools.data.{Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.TestGeoMesaDataStore.{TestAttributeIndex, TestQueryPlan, TestRange}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AttributeIndexTest extends Specification {

  val typeName = "attr-idx-test"
  val spec = "name:String:index=true,age:Int:index=true,height:Float:index=true,dtg:Date,*geom:Point:srid=4326"

  val sft = SimpleFeatureTypes.createType(typeName, spec)

  val df = ISODateTimeFormat.dateTime()

  val aliceGeom   = WKTUtils.read("POINT(45.0 49.0)")
  val billGeom    = WKTUtils.read("POINT(46.0 49.0)")
  val bobGeom     = WKTUtils.read("POINT(47.0 49.0)")
  val charlesGeom = WKTUtils.read("POINT(48.0 49.0)")

  val aliceDate   = df.parseDateTime("2012-01-01T12:00:00.000Z").toDate
  val billDate    = df.parseDateTime("2013-01-01T12:00:00.000Z").toDate
  val bobDate     = df.parseDateTime("2014-01-01T12:00:00.000Z").toDate
  val charlesDate = df.parseDateTime("2014-01-01T12:30:00.000Z").toDate

  val features = Seq(
    Array("alice",   20,   10f, aliceDate,   aliceGeom),
    Array("bill",    21,   11f, billDate,    billGeom),
    Array("bob",     30,   12f, bobDate,     bobGeom),
    Array("charles", null, 12f, charlesDate, charlesGeom)
  ).map { entry =>
    ScalaSimpleFeature.create(sft, entry.head.toString, entry: _*)
  }

  def overlaps(r1: TestRange, r2: TestRange): Boolean = {
    TestGeoMesaDataStore.byteComparator.compare(r1.start, r2.start) match {
      case 0 => true
      case i if i < 0 => TestGeoMesaDataStore.byteComparator.compare(r1.end, r2.start) > 0
      case i if i > 0 => TestGeoMesaDataStore.byteComparator.compare(r2.end, r1.start) > 0
    }
  }

  "AttributeIndex" should {
    "convert shorts to bytes and back" in {
      forall(Seq(0, 32, 127, 128, 129, 255, 256, 257)) { i =>
        val bytes = AttributeIndex.indexToBytes(i)
        bytes must haveLength(2)
        val recovered = AttributeIndex.bytesToIndex(bytes(0), bytes(1))
        recovered mustEqual i
      }
    }

    "correctly set secondary index ranges" in {
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
          writer.write()
        }
      }

      def execute(filter: String, explain: Explainer = ExplainNull): Seq[String] = {
        val q = new Query(typeName, ECQL.toFilter(filter))
        // validate that ranges do not overlap
        foreach(ds.getQueryPlan(q, explainer = explain)) { qp =>
          val ordering = Ordering.comparatorToOrdering(TestGeoMesaDataStore.byteComparator)
          val ranges = qp.asInstanceOf[TestQueryPlan].ranges.sortBy(_.start)(ordering)
          forall(ranges.sliding(2)) { case Seq(left, right) => overlaps(left, right) must beFalse }
        }
        SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).map(_.getID).toSeq
      }

      // height filter matches bob and charles, st filters only match bob
      // this filter illustrates the overlapping range bug GEOMESA-1902
      val stFilter = "bbox(geom, 46.9, 48.9, 48.1, 49.1) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z"

      // expect z3 ranges with the attribute equals prefix
      val results = execute(s"height = 12.0 AND $stFilter")
      results must haveLength(1)
      results must contain("bob")
    }

    "handle functions" in {
      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
          writer.write()
        }
      }

      val filters = Seq (
        "strToUpperCase(name) = 'BILL'",
        "strCapitalize(name) = 'Bill'",
        "strConcat(name, 'foo') = 'billfoo'",
        "strIndexOf(name, 'ill') = 1",
        "strReplace(name, 'ill', 'all', false) = 'ball'",
        "strSubstring(name, 0, 2) = 'bi'",
        "strToLowerCase(name) = 'bill'",
        "strTrim(name) = 'bill'",
        "abs(age) = 21",
        "ceil(age) = 21",
        "floor(age) = 21",
        "'BILL' = strToUpperCase(name)",
        "strToUpperCase('bill') = strToUpperCase(name)",
        "strToUpperCase(name) = strToUpperCase('bill')",
        "name = strToLowerCase('bill')"
      )
      foreach(filters) { filter =>
        val query = new Query(typeName, ECQL.toFilter(filter))
        SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq mustEqual features.slice(1, 2)
      }
    }

    "handle open-ended secondary filters" in {
      val spec = "dtgStart:Date:default=true,dtgEnd:Date:index=true,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType(typeName, spec)

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      val before = "dtgStart BEFORE 2017-01-02T00:00:00.000Z"
      val after = "dtgEnd AFTER 2017-01-03T00:00:00.000Z"
      val filter = ECQL.toFilter(s"$before AND $after")
      val q = new Query(typeName, filter)
      q.getHints.put(QueryHints.QUERY_INDEX, "attr")

      forall(ds.getQueryPlan(q)) { qp =>
        qp.filter.index must beAnInstanceOf[TestAttributeIndex]
        qp.filter.primary must beSome(ECQL.toFilter(after))
        qp.filter.secondary must beSome(ECQL.toFilter(before))
      }
    }

    "handle large or'd attribute queries" in {
      val spec = "attr:String:index=true,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled='attr:1,z3'"
      val sft = SimpleFeatureTypes.createType(typeName, spec)

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      val r = new Random(0L)

      val numFeatures = 5000
      val features = (0 until numFeatures).map { i =>
        val a = (0 until 20).map(_ => r.nextInt(9).toString).mkString + "<foobar>"
        val day = i % 30
        val values = Array[AnyRef](a, f"2014-01-$day%02dT01:00:00.000Z", WKTUtils.read(s"POINT(45.0 45)") )
        val sf = new ScalaSimpleFeature(sft, i.toString, values)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf
      }

      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
          writer.write()
        }
      }

      val dtgPart = "dtg between '2014-01-01T00:00:00.000Z' and '2014-01-31T00:00:00.000Z'"
      val attrPart = "attr in (" + features.take(1000).map(_.getAttribute(0)).map(a => s"'$a'").mkString(", ") + ")"
      val query = new Query(sft.getTypeName, ECQL.toFilter(s"$dtgPart and $attrPart"))

      query.getHints.put(QueryHints.QUERY_INDEX, "attr")

      val start = System.currentTimeMillis()

      val feats = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      while (feats.hasNext) {
        feats.next()
      }

      val time = System.currentTimeMillis() - start

      // set the check fairly high so that we don't get random test failures, but log a warning
      if (time > 500L) {
        System.err.println(s"WARNING: attribute query processing took ${time}ms for large query")
      }
      time must beLessThan(10000L)
    }
  }
}
