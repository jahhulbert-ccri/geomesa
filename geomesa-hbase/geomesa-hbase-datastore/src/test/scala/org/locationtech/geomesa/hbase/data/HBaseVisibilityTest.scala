/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import java.security.PrivilegedExceptionAction

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.security.visibility.VisibilityClient
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.apache.hadoop.hbase.client.security.SecurityCapability
import org.apache.hadoop.hbase.security.User

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class HBaseVisibilityTest extends Specification with LazyLogging {

  sequential

  val cluster = new HBaseTestingUtility()
  var userConnection: Connection = _

  var superUser: User = _
  var testUser: User = _

  step {
    logger.info("Starting embedded hbase")
    cluster.getConfiguration.set("hbase.superuser", "admin")
    cluster.startMiniCluster(1)

    superUser = User.createUserForTesting(cluster.getConfiguration, "admin", Array[String]("supergroup"))
    testUser  = User.createUserForTesting(cluster.getConfiguration, "testuser", Array.empty[String])

    superUser.runAs(new PrivilegedExceptionAction[Unit](){
      override def run(): Unit = {
        cluster.waitTableAvailable(TableName.valueOf("hbase:labels"), 50000)
        val labels = Array[String]("admin", "user", "super")
        val supercon = ConnectionFactory.createConnection(cluster.getConfiguration)
        VisibilityClient.addLabels(supercon, labels)
        cluster.waitLabelAvailable(10000, labels: _*)
        VisibilityClient.setAuths(supercon, Array[String]("admin", "user"), "testuser")
      }
    })

    testUser.runAs(new PrivilegedExceptionAction[Unit](){
      override def run(): Unit = {
        userConnection = ConnectionFactory.createConnection(cluster.getConfiguration)
      }
    })

    logger.info("Started")
  }

  "HBase cluster" should {
    "have vis enabled" >> {
      cluster.getHBaseAdmin.getSecurityCapabilities.asScala must contain(SecurityCapability.CELL_VISIBILITY)
    }
  }

  "HBaseDataStore" should {

    "work with points" in {
      val typeName = "testpoints"

      val params = Map(ConnectionParam.getName -> userConnection, BigTableNameParam.getName -> "test_sft")
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        import org.locationtech.geomesa.security._
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        if (i < 5) sf.visibility = "admin|user|super" else sf.visibility = "(admin|user)&super"
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      forall(Seq(true, false)) { loose =>
        val ds = DataStoreFinder.getDataStore(params ++ Map(LooseBBoxParam.getName -> loose)).asInstanceOf[HBaseDataStore]
        forall(Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
          testQuery(ds, typeName, "INCLUDE", transforms, toAdd.take(5))
          testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
          testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.take(5))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, Seq(toAdd(2), toAdd(3), toAdd(4)))
          testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
          testQuery(ds, typeName, "name = 'name5'", transforms, Seq.empty)
        }
      }

    }

    def testQuery(ds: HBaseDataStore, typeName: String, filter: String, transforms: Array[String], results: Seq[SimpleFeature]) = {
      val query = new Query(typeName, ECQL.toFilter(filter), transforms)
      val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      val features = SelfClosingIterator(fr).toList
      val attributes = Option(transforms).getOrElse(ds.getSchema(typeName).getAttributeDescriptors.map(_.getLocalName).toArray)
      features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
      forall(features) { feature =>
        feature.getAttributes must haveLength(attributes.length)
        forall(attributes.zipWithIndex) { case (attribute, i) =>
          feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
          feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
        }
      }
      ds.getFeatureSource(typeName).getFeatures(query).size() mustEqual results.length
    }

    step {
      logger.info("Stopping embedded hbase")
      cluster.shutdownMiniCluster()

      logger.info("Stopped")
    }
  }
}
