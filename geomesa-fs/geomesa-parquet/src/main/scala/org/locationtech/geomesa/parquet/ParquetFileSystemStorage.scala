/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.{io, util}

import com.google.common.collect.Maps
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.{Failure, Success, Try}

class ParquetFileSystemStorageFactory extends FileSystemStorageFactory {
  override def canProcess(params: util.Map[String, io.Serializable]): Boolean = {
    params.containsKey("fs.path") &&
    params.containsKey("fs.encoding") && params.get("fs.encoding").asInstanceOf[String].equals("parquet")
  }

  override def build(params: util.Map[String, io.Serializable]): FileSystemStorage = {
    val path = params.get("fs.path").asInstanceOf[String]
    val root = new Path(path)
    // TODO: how do we thread configuration through
    new ParquetFileSystemStorage(root, root.getFileSystem(new Configuration))
  }
}

/**
  *
  * @param root the root of this file system for a specifid SimpleFeatureType
  * @param fs
  */
class ParquetFileSystemStorage(root: Path, fs: FileSystem) extends FileSystemStorage with LazyLogging {
  private val featureTypes = {
    val files = fs.listStatus(root)
    val result = Maps.newHashMap[String, SimpleFeatureType]()
    files.map { f =>
      if(!f.isDirectory) Failure(null)
      else Try {
        val in = fs.open(new Path(f.getPath, "schema.sft"))
        val encodedSFT = in.readUTF()
        SimpleFeatureTypes.createType(f.getPath.getName, encodedSFT)
      }
    }.collect { case Success(s) => s }.foreach { sft => result.put(sft.getTypeName, sft) }
    result
  }

  override def listFeatureTypes: util.List[SimpleFeatureType] = {
    import scala.collection.JavaConversions._
    featureTypes.values.toList
  }

  override def getFeatureType(name: String): SimpleFeatureType =  featureTypes.get(name)

  private def buildPartitionList(path: Path, prefix: String): List[String] = {
    val status = fs.listStatus(path)
    status.flatMap { f =>
      if(f.isDirectory) buildPartitionList(f.getPath, s"$prefix${f.getPath.getName}/")
      else {
        if (f.getPath.getName.equals("schema.sft")) List()
        else List(s"$prefix${f.getPath.getName}")
      }
    }.toList
  }

  override def listPartitions(typeName: String): util.List[String] = {
    import scala.collection.JavaConversions._
    buildPartitionList(new Path(root, typeName), "")
  }

  // TODO ask the parition manager the geometry is fully covered?
  override def getPartitionReader(q: Query, partition: Partition): FileSystemPartitionIterator = {
    val sft = featureTypes.get(q.getTypeName)

    import scala.collection.JavaConversions._
    val paths = partition.getPaths.map(p => new Path(this.getFileSystemRoot(q.getTypeName).resolve(p)))

    if (!fs.exists(path)) {
      new EmptyFsIterator(partition)
    }
    else {

      import org.locationtech.geomesa.index.conf.QueryHints._
      val transformSft = q.getHints.getTransformSchema.getOrElse(sft)

      val support = new SimpleFeatureReadSupport(transformSft)
      // TODO: push down predicates and partition pruning
      // TODO ensure that transforms are pushed to the ColumnIO in parquet.
      // TODO: Push down full filter that can't be managed
      val fc = new FilterConverter(transformSft).convert(q.getFilter)
      val parquetFilter =
        fc._1
       .map(FilterCompat.get)
       .getOrElse(FilterCompat.NOOP)

      logger.info(s"Parquet filter: $parquetFilter and modified gt filter ${fc._2}")

      val reader = ParquetReader.builder[SimpleFeature](support, path)
        .withFilter(parquetFilter)
        .build()

      new FilteringIterator(partition, reader, fc._2)
    }
  }



  override def getWriter(featureType: String, partition: String): FileSystemWriter = new FileSystemWriter {
    private val sft = featureTypes.get(featureType)
    private val writer = new SimpleFeatureParquetWriter(new Path(root, new Path(featureType, partition)), new SimpleFeatureWriteSupport(sft))

    override def write(f: SimpleFeature): Unit = writer.write(f)

    override def flush(): Unit = {}

    override def close(): Unit = writer.close()
  }

  override def createNewFeatureType(sft: SimpleFeatureType): Unit = {
    val path = new Path(root, sft.getTypeName)
    fs.mkdirs(path)
    val encoded = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
    val out = fs.create(new Path(path, "schema.sft"))
    out.writeUTF(encoded)
    out.hflush()
    out.hsync()
    out.close()
    featureTypes.put(sft.getTypeName, sft)
  }
}
