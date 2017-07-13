/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.locationtech.geomesa.fs.storage.api.{Metadata, PartitionScheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._


// TODO GEOMESA-1913 Use atomic file writing for metadata
class FileMetadata protected[FileMetadata] (fs: FileSystem,
                                            path: Path,
                                            sft: SimpleFeatureType,
                                            partitionScheme: PartitionScheme,
                                            encoding: String,
                                            partitions: Map[String, List[String]],
                                            conf: Configuration) extends Metadata with LazyLogging {

  private val writeLock = new ReentrantLock()

  private val internalPartitions: ConcurrentHashMap[String, java.util.List[String]] = {
    val m = new ConcurrentHashMap[String, util.List[String]]()
    partitions.entrySet().map { e => m.put(e.getKey, e.getValue)}
    m
  }

  override def addPartition(partition: String, files: java.util.List[String]): Unit = {
    internalPartitions.putIfAbsent(partition, new util.ArrayList[String]())
    internalPartitions.get(partition).addAll(files)
    save()
  }

  override def addPartitions(toAdd: java.util.Map[String, java.util.List[String]]): Unit = {
    toAdd.entrySet().foreach { e =>
      internalPartitions.putIfAbsent(e.getKey, new util.ArrayList[String]())
      internalPartitions.get(e.getKey).addAll(e.getValue)
    }
    save()
  }

  private def save(): Unit = {
    writeLock.lock()
    try {
      val config = ConfigFactory.empty()
        .withValue("featureType", SimpleFeatureTypes.toConfig(sft, includePrefix = false, includeUserData = true).root())
        .withValue("encoding", ConfigValueFactory.fromAnyRef(encoding))
        .withValue("partitionScheme", PartitionScheme.toConfig(partitionScheme).root())
        .withValue("partitions", ConfigValueFactory.fromMap(internalPartitions))

      val out = path.getFileSystem(conf).create(path, true)
      out.writeBytes(config.root.render(ConfigRenderOptions.defaults().setComments(false).setFormatted(true).setJson(true).setOriginComments(false)))
      out.hflush()
      out.hsync()
      out.close()
    } finally {
      writeLock.unlock()
    }
  }

  override def getPartitions: java.util.List[String] = new java.util.ArrayList[String](internalPartitions.keys().toList)

  override def getFiles(partition: String): util.List[String] =
    Collections.unmodifiableList(internalPartitions.get(partition))

  override def getNumStorageFiles: Int = internalPartitions.entrySet().flatMap(_.getValue).size

  override def getNumPartitions: Int = internalPartitions.keys.size

  override def getEncoding: String = encoding

  override def getPartitionScheme: PartitionScheme = partitionScheme

  override def getSimpleFeatureType: SimpleFeatureType = sft

  override def addFile(partition: String, filename: String): Unit = {
    internalPartitions.putIfAbsent(partition, new util.ArrayList[String]())
    internalPartitions.get(partition).add(filename)
    save()
  }

}

object FileMetadata {

  def create(fs: FileSystem,
             path: Path,
             sft: SimpleFeatureType,
             encoding: String,
             partitionScheme: PartitionScheme,
             conf: Configuration): FileMetadata = {
    val m = new FileMetadata(fs, path, sft, partitionScheme, encoding, Map.empty, conf)
    m.save()
    m
  }


  def read(fs: FileSystem, path: Path, conf: Configuration): FileMetadata = {
    val in = fs.open(path)
    val config = try {
      ConfigFactory.parseString(IOUtils.toString(in), ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
    } finally {
      in.close()
    }

    // Load SFT
    val sft = SimpleFeatureTypes.createType(config.getConfig("featureType"), path = None)

    // Load encoding
    val encoding = config.getString("encoding")

    // Load partition scheme
    val scheme = PartitionScheme(sft, config.getConfig("partitionScheme"))

    // Load Partitions
    val partitions = {
      val pconfig = config.getConfig("partitions")
      pconfig.root().entrySet().map { e =>
        val key = e.getKey
        key -> pconfig.getStringList(key).toList

      }.toMap
    }

    new FileMetadata(fs, path, sft, scheme, encoding, partitions, conf)
  }
}