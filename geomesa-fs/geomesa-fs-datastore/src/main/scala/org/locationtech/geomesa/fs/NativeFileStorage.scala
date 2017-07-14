/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.io.Serializable
import java.net.URI
import java.util
import java.util.concurrent.Callable
import java.util.{Collections, ServiceLoader}

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.{FileMetadata, PartitionScheme, StoragePartition}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.JavaConversions._


class NativeFileStorageFactory extends FileSystemStorageFactory {

  override def canProcess(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey("fs.path") &&
      params.containsKey("fs.mode") &&
      params.get("fs.mode").asInstanceOf[String].equals("native")
  }

  override def build(params: util.Map[String, Serializable]): FileSystemStorage = {
    import FileSystemDataStoreParams._
    val path = new Path(PathParam.lookUp(params).asInstanceOf[String])
    val conf = new Configuration()
    val fs = path.getFileSystem(conf)

    new NativeFileStorage(fs, path, conf, params)
  }

}

class NativeFileStorage(fs: FileSystem,
                        val root: Path,
                        conf: Configuration,
                        dsParams: util.Map[String, Serializable]) extends FileSystemStorage {

  import NativeFileStorage._
  private val typeNames: mutable.ListBuffer[String] = {
    val b = mutable.ListBuffer.empty[String]
    if (fs.exists(root)) {
      fs.listStatus(root).filter(_.isDirectory).map(_.getPath.getName).foreach(b += _)
    }
    b
  }

  override def listTypeNames(): util.List[String] = Collections.unmodifiableList(typeNames)

  private def typeStorage(typeName: String): StorageFormat =
    if (typeNames.contains(typeName)) {
      TypeStorageCache.get((root, typeName), new Callable[StorageFormat] {
        override def call(): StorageFormat = {
          val typePath = new Path(root, typeName)
          val metaFile = new Path(typePath, MetadataFileName)
          val metadata = FileMetadata.read(fs, metaFile, conf)
          val fac = FormatFactories.find(_.canLoad(metadata)).getOrElse {
            throw new IllegalArgumentException("Can't create storage factory with the provided params")
          }
          fac.load(fs, typePath, metadata, conf)
        }
      })
    } else {
      throw new IllegalArgumentException(s"Unable to find type $typeName in root directory $root")
    }

  override def getFeatureType(typeName: String): SimpleFeatureType =
    typeStorage(typeName).getMetadata.getSimpleFeatureType

  override def createNewFeatureType(sft: SimpleFeatureType, scheme: PartitionScheme): Unit = {
    val typeName = sft.getTypeName

    if (typeNames.contains(typeName)) {
      throw new IllegalArgumentException(s"Type already exists: ${sft.getTypeName}")
    }

    val encoding: String = if (sft.getUserData.containsKey("fs.encoding")) {
      sft.getUserData.get("fs.encoding").toString
    } else if (dsParams.containsKey("fs.encoding")) {
      dsParams.get("fs.encoding").toString
    } else {
      throw new IllegalArgumentException("Either SimpleFeatureType user-data or data store params must contain key \"fs.encoding\"")
    }

    val params = dsParams.updated("fs.encoding", encoding)

    TypeStorageCache.get((root, typeName), new Callable[StorageFormat] {
      override def call(): StorageFormat = {
        val fac = FormatFactories.find(_.canProcess(params)).getOrElse {
          throw new IllegalArgumentException("Can't create storage factory with the provided params")
        }

        val typePath = new Path(root, typeName)
        val scheme = PartitionScheme.extractFromSft(sft)
        val metaPath = new Path(typePath, MetadataFileName)
        val metadata = FileMetadata.create(fs, metaPath, sft, encoding, scheme, conf)

        val storage = fac.create(fs, typePath, metadata, conf, dsParams)
        typeNames += typeName
        storage
      }
    })

  }

  override def getPartitionScheme(typeName: String): PartitionScheme =
    typeStorage(typeName).getMetadata.getPartitionScheme

  override def getPartition(name: String): Partition = new StoragePartition(name)

  override def listPartitions(typeName: String): util.List[Partition] = {
    typeStorage(typeName).getMetadata.getPartitions.map(getPartition)
  }

  override def getPartitionReader(typeName: String, q: Query, partition: Partition): FileSystemPartitionIterator =
    typeStorage(typeName).getPartitionReader(q, partition)

  override def getWriter(typeName: String, partition: Partition): FileSystemWriter =
    typeStorage(typeName).getWriter(partition)

  override def getPaths(typeName: String, partition: Partition): util.List[Path] =
    typeStorage(typeName).getPaths(partition)

  override def updateMetadata(typeName: String): Unit = {
    val storage = typeStorage(typeName)
    val all = storage.rawListPartitions.map { p =>
      import scala.collection.JavaConverters._
      p.getName -> storage.rawListPaths(p).map(_.getName).asJava
    }.toMap
    storage.getMetadata.addPartitions(all)
  }

  override def getMetadata(typeName: String): Metadata = typeStorage(typeName).getMetadata
}

object NativeFileStorage {
  val MetadataFileName: String = "metadata.json"
  import scala.collection.JavaConversions._
  val FormatFactories = ServiceLoader.load(classOf[StorageFormatFactory]).iterator().toList

  val TypeStorageCache: Cache[(Path, String), StorageFormat] =
    CacheBuilder.newBuilder().build[(Path, String), StorageFormat]()
}
