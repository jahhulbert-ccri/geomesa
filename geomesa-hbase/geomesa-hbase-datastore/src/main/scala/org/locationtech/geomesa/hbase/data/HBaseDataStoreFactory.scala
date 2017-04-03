/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.Serializable

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.security.visibility.VisibilityClient
import org.apache.hadoop.hbase.util.Bytes
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreConfig, _}
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}

import scala.collection.JavaConversions._


class HBaseDataStoreFactory extends DataStoreFactorySpi {

  import HBaseDataStoreParams._

  // TODO: investigate multiple HBase connections per jvm
  private lazy val globalConnection: Connection = {
    val ret = ConnectionFactory.createConnection(HBaseConfiguration.create())
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        ret.close()
      }
    })
    ret
  }

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
    import GeoMesaDataStoreFactory.RichParam

    // TODO HBase Connections don't seem to be Serializable...deal with it
    val connection = ConnectionParam.lookupOpt[Connection](params).getOrElse(globalConnection)

    val remote = RemoteParam.lookupOpt[Boolean](params).getOrElse(false)

    val catalog = BigTableNameParam.lookup[String](params)

    val generateStats = GenerateStatsParam.lookupWithDefault[Boolean](params)
    val audit = if (AuditQueriesParam.lookupWithDefault[Boolean](params)) {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "hbase")
    } else {
      None
    }
    val queryThreads = QueryThreadsParam.lookupWithDefault[Int](params)
    val queryTimeout = GeoMesaDataStoreFactory.queryTimeout(params)
    val looseBBox = LooseBBoxParam.lookupWithDefault[Boolean](params)
    val caching = CachingParam.lookupWithDefault[Boolean](params)
    val security = EnableSecurityParam.lookup[Boolean](params)
    val authsProvider =
      if (security) {
       Some(HBaseDataStoreFactory.buildAuthsProvider(connection, params))
      } else None

    // TODO refactor into buildConfig method
    val config = HBaseDataStoreConfig(
      catalog,
      generateStats,
      audit,
      queryThreads,
      queryTimeout,
      looseBBox,
      caching,
      authsProvider)

    new HBaseDataStore(connection, remote, config)
  }

  override def getDisplayName: String = HBaseDataStoreFactory.DisplayName

  override def getDescription: String = HBaseDataStoreFactory.Description

  override def getParametersInfo: Array[Param] =
    Array(BigTableNameParam, QueryThreadsParam, QueryTimeoutParam, GenerateStatsParam,
      AuditQueriesParam, LooseBBoxParam, CachingParam, EnableSecurityParam, authsParam, forceEmptyAuthsParam)

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean = HBaseDataStoreFactory.canProcess(params)

  override def isAvailable = true

  override def getImplementationHints = null
}

object HBaseDataStoreParams {
  val BigTableNameParam    = new Param("bigtable.table.name", classOf[String], "Table name", true)
  val ConnectionParam      = new Param("connection", classOf[Connection], "Connection", false)
  val RemoteParam        = new Param("remote.filtering", classOf[Boolean], "Remote filtering", false)
  val LooseBBoxParam       = GeoMesaDataStoreFactory.LooseBBoxParam
  val QueryThreadsParam    = GeoMesaDataStoreFactory.QueryThreadsParam
  val GenerateStatsParam   = GeoMesaDataStoreFactory.GenerateStatsParam
  val AuditQueriesParam    = GeoMesaDataStoreFactory.AuditQueriesParam
  val QueryTimeoutParam    = GeoMesaDataStoreFactory.QueryTimeoutParam
  val CachingParam         = GeoMesaDataStoreFactory.CachingParam
  val EnableSecurityParam  = new Param("enableSecurity", classOf[java.lang.Boolean], "Enable HBase Security (Visibilities)", false, false)
  val authsParam           = org.locationtech.geomesa.security.authsParam
  val forceEmptyAuthsParam = org.locationtech.geomesa.security.forceEmptyAuthsParam

}

object HBaseDataStoreFactory {

  import HBaseDataStoreParams._
  val DisplayName = "HBase (GeoMesa)"
  val Description = "Apache HBase\u2122 distributed key/value store"

  case class HBaseDataStoreConfig(catalog: String,
                                  generateStats: Boolean,
                                  audit: Option[(AuditWriter, AuditProvider, String)],
                                  queryThreads: Int,
                                  queryTimeout: Option[Long],
                                  looseBBox: Boolean,
                                  caching: Boolean,
                                  authProvider: Option[AuthorizationsProvider]) extends GeoMesaDataStoreConfig

  def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    params.containsKey(BigTableNameParam.key)

  def buildAuthsProvider(connection: Connection, params: java.util.Map[String, Serializable]): AuthorizationsProvider = {

    val forceEmptyOpt: Option[java.lang.Boolean] = security.forceEmptyAuthsParam.lookupOpt[java.lang.Boolean](params)
    val forceEmptyAuths = forceEmptyOpt.getOrElse(java.lang.Boolean.FALSE).asInstanceOf[Boolean]

    if (!VisibilityClient.isCellVisibilityEnabled(connection)) {
      throw new IllegalArgumentException("HBase cell visibility is not enabled on cluster")
    }

    // master auths is the superset of auths this connector/user can support
    val userName = User.getCurrent.getName
    val masterAuths = VisibilityClient.getAuths(connection, userName).getAuthList.map(a => Bytes.toString(a.toByteArray))

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = authsParam.lookupOpt[String](params).getOrElse("").split(",").filter(s => !s.isEmpty)

    // verify that the configured auths are valid for the connector we are using (fail-fast)
    val invalidAuths = configuredAuths.filterNot(masterAuths.contains)
    if (invalidAuths.nonEmpty) {
      throw new IllegalArgumentException(s"The authorizations '${invalidAuths.mkString(",")}' " +
        "are not valid for the HBase user and connection being used")
    }

    // if the caller provided any non-null string for authorizations, use it;
    // otherwise, grab all authorizations to which the Accumulo user is entitled
    if (configuredAuths.length != 0 && forceEmptyAuths) {
      throw new IllegalArgumentException("Forcing empty auths is checked, but explicit auths are provided")
    }
    val auths: List[String] =
      if (forceEmptyAuths || configuredAuths.length > 0) configuredAuths.toList
      else masterAuths.toList

    security.getAuthorizationsProvider(params, auths)
  }
}
