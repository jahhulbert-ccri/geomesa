/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.converter

import java.io.Serializable
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.Query
import org.locationtech.geomesa.convert.{SimpleFeatureConverter, SimpleFeatureConverters}
import org.locationtech.geomesa.fs.storage.api.{FileSystemPartitionIterator, FileSystemStorage, FileSystemStorageFactory, FileSystemWriter}
import org.locationtech.geomesa.fs.{DateScheme, PartitionScheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.opengis.feature.simple.SimpleFeatureType


class ConverterStorageFactory extends FileSystemStorageFactory {
  override def canProcess(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey("fs.path") &&
    params.containsKey("fs.encoding") && params.get("fs.encoding").asInstanceOf[String].equals("converter")
  }

  override def build(params: util.Map[String, Serializable]): FileSystemStorage = {
    val path = params.get("fs.path").asInstanceOf[String]
    val root = new Path(path)

    val converterName = params.get("fs.options.converter.name").toString
    val sftName = params.get("fs.options.sft.name").toString

    val sft = SimpleFeatureTypeLoader.sftForName(sftName).getOrElse({
      throw new IllegalArgumentException(s"Unable to load sft name $sftName")
    })
    val converter = SimpleFeatureConverters.build(sft, converterName)

    val partitionScheme = new DateScheme(DateTimeFormatter.ofPattern("yyyy/DDD/HH/mm"), ChronoUnit.MINUTES, 15, sft, "dtg")
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

  override def createNewFeatureType(sft: SimpleFeatureType): Unit =
    throw new UnsupportedOperationException("Cannot create new feature type on existing DB")

  override def getFeatureType(name: String): SimpleFeatureType =
    if (sft.getTypeName != name) {
      throw new IllegalArgumentException(s"Type $name doesn't match configured sft name ${sft.getTypeName}")
    } else {
      sft
    }

  override def getWriter(typeName: String, partition: String): FileSystemWriter =
    throw new UnsupportedOperationException("Cannot append to converter datastore")

  override def getPartitionReader(q: Query, partition: String): FileSystemPartitionIterator =
    new ConverterPartitionReader(root, partition, sft, converter, q.getFilter)

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
    buildPartitionList(root, "")
  }
}
