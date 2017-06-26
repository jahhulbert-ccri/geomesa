/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.{io, util}

import com.google.common.collect.Maps
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.{DateTimeZ2Scheme, LeafStoragePartition}
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
class ParquetFileSystemStorage(root: Path,
                               fs: FileSystem) extends FileSystemStorage with LazyLogging {

  private val fileExtension = "parquet"

  private val featureTypes = {
    val files = fs.listStatus(root)
    val result = Maps.newHashMap[String, SimpleFeatureType]()
    files.map { f =>
      if (!f.isDirectory) Failure(null)
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
    import scala.collection.JavaConversions._
    buildPartitionList(new Path(root, typeName), "", 0,
      getPartitionScheme(featureTypes(typeName)).maxDepth()).map(getPartition)
  }

  // TODO ask the parition manager the geometry is fully covered?
  override def getPartitionReader(q: Query, partition: Partition): FileSystemPartitionIterator = {
    val sft = featureTypes.get(q.getTypeName)
    // TODO in the future there may be multiple files
    val path = new Path(new Path(root, sft.getTypeName), partition.getPaths.get(0).toString)

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



  override def getWriter(featureType: String, partition: Partition): FileSystemWriter = new FileSystemWriter {
    private val sft = featureTypes.get(featureType)

    private val featureRoot = new Path(root, featureType)
    // TODO in the future there may be multiple files
    private val dataPath    = new Path(featureRoot, partition.getPaths.get(0).toString)

    private val conf = {
      val c = new Configuration()
      c.set("sft.name", sft.getTypeName)
      c.set("sft.spec", SimpleFeatureTypes.encodeType(sft, true))
      c
    }
    private val writer = new SimpleFeatureParquetWriter(dataPath, conf)

    override def write(f: SimpleFeature): Unit = writer.write(f)

    override def flush(): Unit = {}

    override def close(): Unit = writer.close()
  }

  override def createNewFeatureType(sft: SimpleFeatureType, partitionScheme: PartitionScheme): Unit = {
    val path = new Path(root, sft.getTypeName)
    fs.mkdirs(path)
    val encoded = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
    val out = fs.create(new Path(path, "schema.sft"))
    out.writeUTF(encoded)
    out.hflush()
    out.hsync()
    out.close()
    featureTypes.put(sft.getTypeName, sft)

    // TODO write scheme for partition
  }

  override def getFileSystemRoot(typeName: String): URI = root.toUri

  override def getPartitionScheme(sft: SimpleFeatureType): PartitionScheme = {
    // TODO allow other things
    new DateTimeZ2Scheme(DateTimeFormatter.ofPattern("yyyy/DDD/HH"), ChronoUnit.HOURS, 1, 10, sft, "dtg", "geom")
  }

  override def getPartition(name: String): Partition = new LeafStoragePartition(name, Some(fileExtension))
}
