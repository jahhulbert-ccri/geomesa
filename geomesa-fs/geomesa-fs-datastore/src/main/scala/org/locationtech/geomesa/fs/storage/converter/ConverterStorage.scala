/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.io.Serializable
import java.net.URI
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.Query
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigResolver, SimpleFeatureConverter, SimpleFeatureConverters}
import org.locationtech.geomesa.fs.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.{LeafStoragePartition, PartitionScheme}
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}
import org.opengis.feature.simple.SimpleFeatureType


class ConverterStorageFactory extends FileSystemStorageFactory {
  override def canProcess(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey("fs.path") &&
    params.containsKey("fs.encoding") &&
      params.get("fs.encoding").asInstanceOf[String].equals("converter")
  }

  override def build(params: util.Map[String, Serializable]): FileSystemStorage = {
    val path = params.get("fs.path").asInstanceOf[String]
    val root = new Path(path)

    val sftArg = Option(FileSystemDataStoreParams.SftConfigParam.lookUp(params))
      .orElse(Option(FileSystemDataStoreParams.SftNameParam.lookUp(params)))
      .map(_.asInstanceOf[String])
      .getOrElse( throw new IllegalArgumentException(s"Must provide sft config or name"))

    val sft = SftArgResolver.getArg(SftArgs(sftArg, null)) match {
      case Left(e) => throw e
      case Right(sftype) => sftype
    }

    val convertArg = Option(FileSystemDataStoreParams.ConverterConfigParam.lookUp(params)).
      orElse(Option(FileSystemDataStoreParams.ConverterNameParam.lookUp(params)))
      .map(_.asInstanceOf[String])
      .getOrElse(throw new IllegalArgumentException(s"Must provide either converter config or name"))

    val converterConfig = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
      case Left(e) => throw e
      case Right(conf) => conf
    }
    val converter = SimpleFeatureConverters.build(sft, converterConfig)

    val partitionScheme = PartitionScheme(sft, params)
    new ConverterStorage(root, root.getFileSystem(new Configuration), partitionScheme, sft, converter)
  }
}


class ConverterStorage(root: Path,
                       fs: FileSystem,
                       partitionScheme: PartitionScheme,
                       sft: SimpleFeatureType,
                       converter: SimpleFeatureConverter[_]) extends FileSystemStorage {
  override def listFeatureTypes(): util.List[SimpleFeatureType] = {
    import scala.collection.JavaConverters._
    List(sft).asJava
  }

  override def createNewFeatureType(sft: SimpleFeatureType, partitionScheme: PartitionScheme): Unit =
    throw new UnsupportedOperationException("Cannot create new feature type on existing DB")

  override def getFeatureType(name: String): SimpleFeatureType =
    if (sft.getTypeName != name) {
      throw new IllegalArgumentException(s"Type $name doesn't match configured sft name ${sft.getTypeName}")
    } else {
      sft
    }

  override def getWriter(typeName: String, partition: Partition): FileSystemWriter =
    throw new UnsupportedOperationException("Cannot append to converter datastore")

  override def getPartitionReader(q: Query, partition: Partition): FileSystemPartitionIterator =
    new ConverterPartitionReader(root, partition, sft, converter, q.getFilter)

  private def buildPartitionList(path: Path, prefix: String, curDepth: Int): List[String] = {
    if (curDepth > partitionScheme.maxDepth()) return List.empty[String]
    val status = fs.listStatus(path)
    status.flatMap { f =>
      if (f.isDirectory) buildPartitionList(f.getPath, s"$prefix${f.getPath.getName}/", curDepth + 1)
      else {
        if (f.getPath.getName.equals("schema.sft")) List()
        else List(s"$prefix${f.getPath.getName}")
      }
    }.toList
  }

  override def listPartitions(typeName: String): util.List[Partition] = {
    import scala.collection.JavaConversions._
    buildPartitionList(root, "", 0).map(getPartition)
  }

  override def getFileSystemRoot(typeName: String): URI = root.toUri

  override def getPartitionScheme(typeName: String): PartitionScheme = partitionScheme

  override def getPartition(name: String): Partition = new LeafStoragePartition(name)

  import scala.collection.JavaConversions._
  override def getPaths(typeName: String, partition: Partition): java.util.List[URI] =
    List(new Path(root, partition.getName).toUri)

  override def newPartitions(`type`: String, partitionNames: util.List[String]): Unit = ???
}
