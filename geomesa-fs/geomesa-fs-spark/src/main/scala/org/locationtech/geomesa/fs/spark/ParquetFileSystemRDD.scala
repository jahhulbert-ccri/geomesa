/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import java.io.Serializable
import java.util
import java.util.ServiceLoader

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory
import org.locationtech.geomesa.fs.{FileSystemDataStoreFactory, PartitionUtils}
import org.locationtech.geomesa.parquet.{FilterConverter, ParquetFileSystemStorageFactory, SFParquetInputFormat, SimpleFeatureReadSupport}
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.opengis.feature.simple.SimpleFeature

/**
  * Created by ahulbert on 6/28/17.
  */
class ParquetFileSystemRDD extends SpatialRDDProvider with LazyLogging {

  override def canProcess(params: util.Map[String, Serializable]): Boolean =
    new ParquetFileSystemStorageFactory().canProcess(params)

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): SpatialRDD = {
    val fac = new FileSystemDataStoreFactory()

    import scala.collection.JavaConversions._
    val ds = fac.createDataStore(params)
    val origSft = ds.getSchema(query.getTypeName)
    import org.locationtech.geomesa.index.conf.QueryHints._
    val sft = query.getHints.getTransformSchema.getOrElse(origSft)
    val fc = new FilterConverter(origSft).convert(query.getFilter)._1

    val storage = ServiceLoader.load(classOf[FileSystemStorageFactory]).iterator().filter(_.canProcess(params)).map(_.build(params)).next()
    val inputPaths = PartitionUtils.getPartitionsForQuery(storage, origSft, query).flatMap { p =>
      storage.getPaths(sft.getTypeName, p).map(new Path(_))
    }

    // note: file input format requires a job object, but conf gets copied in job object creation,
    // so we have to copy the file paths back out
    val job = Job.getInstance(conf)

    // Note we have to copy all the conf twice?
    FileInputFormat.setInputPaths(job, inputPaths: _*)
    conf.set(FileInputFormat.INPUT_DIR, job.getConfiguration.get(FileInputFormat.INPUT_DIR))

    // Note we have to copy all the conf twice?
    SimpleFeatureReadSupport.updateConf(sft, job.getConfiguration)
    SimpleFeatureReadSupport.updateConf(sft, conf)

    // Note we have to copy all the conf twice?
    fc.foreach(ParquetInputFormat.setFilterPredicate(job.getConfiguration, _))
    fc.foreach(ParquetInputFormat.setFilterPredicate(conf, _))

    // Note we have to copy all the conf twice?
    ParquetInputFormat.setReadSupportClass(job, classOf[SimpleFeatureReadSupport])
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, job.getConfiguration.get(ParquetInputFormat.READ_SUPPORT_CLASS))

    // Note we have to copy all the conf twice?
    SFParquetInputFormat.setGeoToolsFilter(job.getConfiguration, query.getFilter)
    conf.set(SFParquetInputFormat.FilterKey, job.getConfiguration.get(SFParquetInputFormat.FilterKey))

    val rdd = sc.newAPIHadoopRDD(conf, classOf[SFParquetInputFormat], classOf[Void], classOf[SimpleFeature])
    SpatialRDD(rdd.map(_._2), sft)
  }

  override def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit =
    throw new NotImplementedError("Converter provider is read-only")
}
