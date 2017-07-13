/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.net.URI
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.{FileMetadata, StoragePartition, StorageUtils}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ParquetFileSystemStorageFactory extends FileSystemStorageFactory {
  import ParquetFileSystemStorage._


  override def canProcess(params: util.Map[String, String]): Boolean = {
    params.containsKey("fs.path") &&
    params.containsKey("fs.encoding") && params.get("fs.encoding").asInstanceOf[String].equals("parquet")
  }

  override def create(path: URI,
                      sft: SimpleFeatureType,
                      partitionScheme: PartitionScheme,
                      params: util.Map[String, String]): FileSystemStorage = {
    val typePath = new Path(path)
    val conf = new Configuration
    val fs = typePath.getFileSystem(conf)
    if (params.containsKey("parquet.compression")) {
      conf.set("parquet.compression", params.get("parquet.compression").asInstanceOf[String])
    } else if (System.getProperty("parquet.compression") != null) {
      conf.set("parquet.compression", System.getProperty("parquet.compression"))
    }

    val metaFile = FileMetadata.create(fs, new Path(typePath, MetaFileName), sft, "parquet", partitionScheme, conf)
    new ParquetFileSystemStorage(fs, typePath, metaFile, conf)
  }

  override def load(path: URI, params: util.Map[String, String]): FileSystemStorage = {
    val typePath = new Path(path)
    val conf = new Configuration
    val fs = typePath.getFileSystem(conf)
    val metaFile = FileMetadata.read(fs, new Path(typePath, MetaFileName), conf)
    new ParquetFileSystemStorage(fs, typePath, metaFile, conf)
  }
}

///**
//  *
//  * @param root the root of this file system for a specifid SimpleFeatureType
//  * @param fs
//  */
//class x(root: Path,
//                               fs: FileSystem,
//                               conf: Configuration) extends LazyLogging {
//
//  import ParquetFileSystemStorage._
//
//  private val typeNames: mutable.ListBuffer[String] = {
//    val b = mutable.ListBuffer.empty[String]
//    if (fs.exists(root)) {
//      fs.listStatus(root).filter(_.isDirectory).map(_.getPath.getName).foreach(b += _)
//    }
//    b
//  }
//
//  private def typeStorage(typeName: String): ParquetTypeStorage =
//    if (typeNames.contains(typeName)) {
//      TypeStorageCache.get((root, typeName), new Callable[ParquetTypeStorage] {
//        override def call(): ParquetTypeStorage = {
//          val typePath = new Path(root, typeName)
//          val metaFile = FileMetadata.read(fs, new Path(typePath, MetaFileName), conf)
//          new ParquetTypeStorage(fs, typePath, metaFile, conf)
//        }
//      })
//    } else {
//      throw new IllegalArgumentException(s"Unable to find type $typeName in root directory $root")
//    }
//
//  override def listFeatureTypes: util.List[SimpleFeatureType] = {
//    import scala.collection.JavaConversions._
//    TypeStorageCache.asMap().filter(_._1._1 == root).map(_._2.sft).toList
//  }
//
//  override def getFeatureType(name: String): SimpleFeatureType = typeStorage(name).sft
//
//  override def listPartitions(typeName: String): util.List[Partition] =
//    typeStorage(typeName).listPartitions
//
//  override def getPartitionReader(typeName: String, q: Query, partition: Partition): FileSystemPartitionIterator =
//    typeStorage(typeName).getPartitionReader(q, partition)
//
//  override def getWriter(typeName: String, partition: Partition): FileSystemWriter =
//    typeStorage(typeName).getWriter(partition)
//
//  override def createNewFeatureType(sft: SimpleFeatureType, partitionScheme: PartitionScheme): Unit = {
//    val typeName = sft.getTypeName
//    if (typeNames.contains(typeName)) {
//      throw new IllegalArgumentException(s"Type already exists: ${sft.getTypeName}")
//    } else {
//      TypeStorageCache.get((root, typeName), new Callable[ParquetTypeStorage] {
//        override def call(): ParquetTypeStorage = {
//          val typePath = new Path(root, typeName)
//          val metaPath = new Path(typePath, MetaFileName)
//          val metaFile = FileMetadata.create(fs, metaPath, sft, "parquet", partitionScheme, conf)
//          typeNames += typeName
//          new ParquetTypeStorage(fs, typePath, metaFile, conf)
//        }
//      })
//    }
//  }
//
//  override def getFileSystemRoot(typeName: String): URI = root.toUri
//
//  override def getPartitionScheme(typeName: String): PartitionScheme = {
//    typeStorage(typeName).scheme
//  }
//
//  override def getPartition(name: String): Partition = new StoragePartition(name)
//
//  override def getPaths(typeName: String, partition: Partition): java.util.List[URI] =
//    typeStorage(typeName).getPaths(partition)
//
//  override def getMetadata(typeName: String): Metadata = typeStorage(typeName).metadata
//
//  override def updateMetadata(typeName: String): Unit = ???
//
//}

object ParquetFileSystemStorage {

  val DataFileExtension = "parquet"
  val SchemaFileName    = "schema.sft"
  val MetaFileName      = "metadata.json"

//  val TypeStorageCache: Cache[(Path, String), ParquetTypeStorage] = CacheBuilder.newBuilder().build[(Path, String), ParquetTypeStorage]()
}

class ParquetFileSystemStorage(fs: FileSystem,
                               root: Path,
                               val metadata: Metadata,
                               conf: Configuration) extends FileSystemStorage with LazyLogging {

  import ParquetFileSystemStorage._

  val sft: SimpleFeatureType = metadata.getSimpleFeatureType
  val scheme: PartitionScheme = metadata.getPartitionScheme
  val typeName: String = sft.getTypeName

  def listPartitions: util.List[Partition] = {
    import scala.collection.JavaConversions._
    metadata.getPartitions.map(getPartition)
  }


  // TODO ask the parition manager the geometry is fully covered?
  def getPartitionReader(q: Query, partition: Partition): FileSystemPartitionIterator = {

    import org.locationtech.geomesa.index.conf.QueryHints._
    QueryPlanner.setQueryTransforms(q, sft)

    val transformSft = q.getHints.getTransformSchema.getOrElse(sft)

    // TODO: push down predicates and partition pruning
    // TODO ensure that transforms are pushed to the ColumnIO in parquet.
    // TODO: Push down full filter that can't be managed
    val fc = new FilterConverter(transformSft).convert(q.getFilter)
    val parquetFilter =
      fc._1
        .map(FilterCompat.get)
        .getOrElse(FilterCompat.NOOP)

    logger.info(s"Parquet filter: $parquetFilter and modified gt filter ${fc._2}")

    import scala.collection.JavaConversions._
    val iters = getPaths(partition).toIterator.map(u => new Path(u)).map { path =>
      if (!fs.exists(path)) {
        new EmptyFsIterator(partition)
      }
      else {
        val support = new SimpleFeatureReadSupport
        SimpleFeatureReadSupport.setSft(transformSft, conf)

        conf.set("parquet.filter.dictionary.enabled", "true")
        val builder = ParquetReader.builder[SimpleFeature](support, path)
          .withFilter(parquetFilter)
          .withConf(conf)

        new FilteringIterator(partition, builder, fc._2)
      }
    }
    new MultiIterator(partition, iters)
  }

  def getWriter(partition: Partition): FileSystemWriter = {
    new FileSystemWriter {
      private val leaf     = scheme.isLeafStorage
      private val dataPath = StorageUtils.nextFile(fs, root, partition.getName, leaf, DataFileExtension)
      metadata.addFile(partition.getName, dataPath.getName)

      private val sftConf = {
        val c = new Configuration(conf)
        SimpleFeatureReadSupport.setSft(sft, c)
        if (conf.get("parquet.compression") == null) {
          conf.set("parquet.compression", CompressionCodecName.SNAPPY.name())
        }
        c
      }

      private val writer = SimpleFeatureParquetWriter.builder(dataPath, sftConf).build()

      override def write(f: SimpleFeature): Unit = writer.write(f)

      override def flush(): Unit = {}

      override def close(): Unit = {

        CloseQuietly(writer)
      }
    }
  }

  def getPartition(name: String): Partition = new StoragePartition(name)

  private def listStorageFiles: util.Map[String, util.List[String]] = {
    val partitions =
      StorageUtils.buildPartitionList(fs, root, scheme, StorageUtils.SequenceLength, DataFileExtension)
        .map(getPartition)
    import scala.collection.JavaConverters._
    partitions.map { p =>
      val files = StorageUtils.listStorageFiles(fs, root, p, scheme.isLeafStorage, DataFileExtension).map(_.getName).asJava
      p.getName -> files
    }.toMap.asJava
  }

  def getPaths(partition: Partition): java.util.List[URI] = {
    val baseDir = if (scheme.isLeafStorage) {
      StorageUtils.partitionPath(root, partition.getName).getParent
    } else {
      StorageUtils.partitionPath(root, partition.getName)
    }
    import scala.collection.JavaConversions._
    val files = metadata.getFiles(partition.getName)
    files.map(new Path(baseDir, _)).map(_.toUri)
  }

  override def updateMetadata(): Unit = ???

  override def getMetadata: Metadata = metadata
}