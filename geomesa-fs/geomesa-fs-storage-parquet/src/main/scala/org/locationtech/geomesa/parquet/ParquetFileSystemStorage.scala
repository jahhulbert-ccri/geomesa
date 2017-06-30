/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.net.URI
import java.{io, util}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.LeafStoragePartition
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class ParquetFileSystemStorageFactory extends FileSystemStorageFactory {
  override def canProcess(params: util.Map[String, io.Serializable]): Boolean = {
    params.containsKey("fs.path") &&
    params.containsKey("fs.encoding") && params.get("fs.encoding").asInstanceOf[String].equals("parquet")
  }

  override def build(params: util.Map[String, io.Serializable]): FileSystemStorage = {
    val path = params.get("fs.path").asInstanceOf[String]
    val root = new Path(path)
    val conf = new Configuration
    new ParquetFileSystemStorage(root, root.getFileSystem(conf), conf)
  }
}

/**
  *
  * @param root the root of this file system for a specifid SimpleFeatureType
  * @param fs
  */
class ParquetFileSystemStorage(root: Path,
                               fs: FileSystem,
                               conf: Configuration) extends FileSystemStorage with LazyLogging {

  private val fileExtension = "parquet"
  private val schemeConfFile = "schema.sft"
  private val metaFile = "metadata"

  private val metaLoader = new CacheLoader[String, Metadata] {
    override def load(k: String): Metadata = {
      val path = new Path(new Path(root, k), metaFile)
      new Metadata(path, conf)
    }
  }
  private val metaData =
    CacheBuilder.newBuilder()
      .build[String, Metadata](metaLoader)

  override def newPartitions(typeName: String, partitionNames: util.List[String]): Unit = {
    import scala.collection.JavaConversions._
    metaData(typeName).add(partitionNames)
  }

  // TODO we don't necessarily want the s3 bucket path to exist...but need to verify we can write
  private val featureTypes = {
    val files = if (fs.exists(root)) fs.listStatus(root) else Array.empty[FileStatus]
    val result = mutable.HashMap.empty[String, SimpleFeatureType]
    files.map { f =>
      if (!f.isDirectory) Failure(null)
      else Try {
        val in = fs.open(new Path(f.getPath, schemeConfFile))
        val sftConf = ConfigFactory.parseString(in.readUTF())
        SimpleFeatureTypes.createType(sftConf)
      }
    }.collect { case Success(s) => s }
      .foreach { sft => result += sft.getTypeName -> sft }
    result
  }

  override def listFeatureTypes: util.List[SimpleFeatureType] = {
    import scala.collection.JavaConversions._
    featureTypes.values.toList
  }

  override def getFeatureType(name: String): SimpleFeatureType =  featureTypes(name)

  private def buildPartitionList(path: Path, prefix: String, curDepth: Int, maxDepth: Int): List[String] = {
    if (curDepth > maxDepth) return List.empty[String]
    val status = fs.listStatus(path)
    status.flatMap { f =>
      if (f.isDirectory) {
        buildPartitionList(f.getPath,  s"$prefix${f.getPath.getName}/", curDepth + 1, maxDepth)
      } else {
        if (f.getPath.getName.equals("schema.sft")) List()
        else {
          val name = f.getPath.getName.dropRight(fileExtension.length + 1)
          List(s"$prefix$name")
        }
      }
    }.toList
  }

  override def listPartitions(typeName: String): util.List[Partition] = {
//    import scala.collection.JavaConversions._
//    buildPartitionList(new Path(root, typeName), "", 0,
//      getPartitionScheme(typeName).maxDepth()).map(getPartition)
    import scala.collection.JavaConversions._
    metaData(typeName).getPartitions.map(getPartition)
  }

  // TODO ask the parition manager the geometry is fully covered?
  override def getPartitionReader(q: Query, partition: Partition): FileSystemPartitionIterator = {
    val sft = featureTypes(q.getTypeName)

    // TODO in the future there may be multiple files
    val path = new Path(getPaths(sft.getTypeName, partition).get(0))

    if (!fs.exists(path)) {
      new EmptyFsIterator(partition)
    }
    else {
      import org.locationtech.geomesa.index.conf.QueryHints._
      val transformSft = q.getHints.getTransformSchema.getOrElse(sft)

      val support = new SimpleFeatureReadSupport
      SimpleFeatureReadSupport.updateConf(transformSft, conf)

      // TODO: push down predicates and partition pruning
      // TODO ensure that transforms are pushed to the ColumnIO in parquet.
      // TODO: Push down full filter that can't be managed
      val fc = new FilterConverter(transformSft).convert(q.getFilter)
      val parquetFilter =
        fc._1
       .map(FilterCompat.get)
       .getOrElse(FilterCompat.NOOP)

      logger.info(s"Parquet filter: $parquetFilter and modified gt filter ${fc._2}")

      logger.info(s"Opening reader for partition $partition")
      val reader = ParquetReader.builder[SimpleFeature](support, path)
        .withFilter(parquetFilter)
        .withConf(conf)
        .build()

      new FilteringIterator(partition, reader, fc._2)
    }
  }

  override def getWriter(featureType: String, partition: Partition): FileSystemWriter =
    new FileSystemWriter {
      private val sft = featureTypes(featureType)

      // TODO in the future there may be multiple files
      val dataPath = new Path(getPaths(sft.getTypeName, partition).get(0))

      private val sftConf = {
        val c = new Configuration(conf)
        SimpleFeatureReadSupport.updateConf(sft, c)
        c
      }

      private val writer = new SimpleFeatureParquetWriter(dataPath, sftConf)

      override def write(f: SimpleFeature): Unit = writer.write(f)

      override def flush(): Unit = {}

      override def close(): Unit = {
        writer.close()
      }
    }

  override def createNewFeatureType(sft: SimpleFeatureType, partitionScheme: PartitionScheme): Unit = {
    org.locationtech.geomesa.fs.storage.common.PartitionScheme.addToSft(sft, partitionScheme)
    val path = new Path(root, sft.getTypeName)
    fs.mkdirs(path)
    val encoded = SimpleFeatureTypes.toConfigString(sft, includeUserData = true, concise = false, includePrefix = false)
    val out = fs.create(new Path(path, schemeConfFile))
    out.writeBytes(encoded)
    out.hflush()
    out.hsync()
    out.close()
    featureTypes.put(sft.getTypeName, sft)
  }

  override def getFileSystemRoot(typeName: String): URI = root.toUri

  override def getPartitionScheme(typeName: String): PartitionScheme = {
    val sft = featureTypes(typeName)
    val conf = sft.getUserData.get(org.locationtech.geomesa.fs.storage.common.PartitionScheme.PartitionSchemeKey).asInstanceOf[String]
    org.locationtech.geomesa.fs.storage.common.PartitionScheme(sft, ConfigFactory.parseString(conf))
  }

  override def getPartition(name: String): Partition = new LeafStoragePartition(name)

  import scala.collection.JavaConversions._
  override def getPaths(typeName: String, partition: Partition): java.util.List[URI] =
    List(new Path(new Path(root, typeName), partition.getName).suffix(s".$fileExtension").toUri)
}
