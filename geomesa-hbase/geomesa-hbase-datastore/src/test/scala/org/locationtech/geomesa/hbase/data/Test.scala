//package org.locationtech.geomesa.hbase.data
//
//import org.apache.hadoop.hbase.security.visibility.VisibilityClient
//import org.geotools.data.DataStoreFinder
//import org.geotools.factory.Hints
//import org.locationtech.geomesa.features.ScalaSimpleFeature
//import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.Params._
//import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
//import org.opengis.feature.simple.SimpleFeatureType
//import org.opengis.filter.Filter
//
//import scala.collection.JavaConversions._
//
///**
//  * Created by hulbert on 3/21/17.
//  */
//object Test {
//
//  def main(args: Array[String]): Unit = {
//    val typeName = "testpoints"
//    val params = Map(BigTableNameParam.getName -> "vis_test")
//    lazy val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
//
//
//
////    val conf = HBaseConfiguration.create();
////    conf.set("hbase.zookeeper.property.clientPort", "2181");
////    conf.set("hbase.zookeeper.quorum", "localhost");
////    conf.set("zookeeper.znode.parent", "/hbase");
////    val conn = ConnectionFactory.createConnection(conf);
////
////    import GeoMesaDataStoreFactory.RichParam
////
////
////    val catalog = BigTableNameParam.lookup[String](params)
////
////    val generateStats = GenerateStatsParam.lookupWithDefault[Boolean](params)
////    val audit = if (AuditQueriesParam.lookupWithDefault[Boolean](params)) {
////      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "hbase")
////    } else {
////      None
////    }
////    val queryThreads = QueryThreadsParam.lookupWithDefault[Int](params)
////    val queryTimeout = GeoMesaDataStoreFactory.queryTimeout(params)
////    val looseBBox = LooseBBoxParam.lookupWithDefault[Boolean](params)
////    val caching = CachingParam.lookupWithDefault[Boolean](params)
////    val authsProvider = HBaseDataStoreFactory.buildAuthsProvider
////
////    // TODO refactor into buildConfig method
////    val config = HBaseDataStoreConfig(
////      catalog,
////      generateStats,
////      audit,
////      queryThreads,
////      queryTimeout,
////      looseBBox,
////      caching,
////      authsProvider)
////
////    val ds = new HBaseDataStore(conn, config)
//
//    def createFeatures(sft: SimpleFeatureType) = (0 until 10).map { i =>
//      val sf = new ScalaSimpleFeature(i.toString, sft)
//      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
//      sf.setAttribute(0, s"name $i")
//      sf.setAttribute(1, s"2014-01-01T0$i:00:01.000Z")
//      sf.setAttribute(2, s"POINT(4$i 5$i)")
//      sf
//    }
//
//    ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String,dtg:Date,*geom:Point:srid=4326"))
//    val sft = ds.getSchema(typeName)
//    println(SimpleFeatureTypes.encodeType(sft, includeUserData = true))
//
//    println(SimpleFeatureTypes.encodeType(sft, includeUserData = true))
//
//    val features = createFeatures(sft)
//
//    val fs = ds.getFeatureSource(typeName)
//
//    val enabled = VisibilityClient.isCellVisibilityEnabled(ds.connection)
//    println(s"enabled: $enabled")
////    val ids = fs.addFeatures(new ListFeatureCollection(sft, features))
//
//    val ret = fs.getFeatures(Filter.INCLUDE)
//    println(ret.size())
//  }
//}
