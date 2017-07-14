/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.io.Serializable
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.{StoragePartition, StorageUtils}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._


class ParquetStorageFormatFactory extends StorageFormatFactory {

  import ParquetStorageFormat._
  override def canProcess(params: util.Map[String, Serializable]): Boolean =
    params.containsKey("fs.encoding") &&
      params.get("fs.encoding").toString == ParquetEncoding


  override def create(fs: FileSystem,
                      path: Path,
                      metadata: Metadata,
                      conf: Configuration,
                      params: util.Map[String, Serializable]): StorageFormat = {

    if (params.containsKey("parquet.compression")) {
      conf.set("parquet.compression", params.get("parquet.compression").asInstanceOf[String])
    } else if (System.getProperty("parquet.compression") != null) {
      conf.set("parquet.compression", System.getProperty("parquet.compression"))
    }

    conf.set("parquet.filter.dictionary.enabled", "true")
    if (conf.get("parquet.compression") == null) {
      conf.set("parquet.compression", CompressionCodecName.SNAPPY.name())
    }

    new ParquetStorageFormat(fs, path, metadata, conf)
  }

    override def load(fs: FileSystem,
                      path: Path,
                      metadata: Metadata,
                      conf: Configuration): StorageFormat = {
      conf.set("parquet.filter.dictionary.enabled", "true")
      if (conf.get("parquet.compression") == null) {
        conf.set("parquet.compression", CompressionCodecName.SNAPPY.name())
      }
      new ParquetStorageFormat(fs, path, metadata, conf)
    }

    override def canLoad(metadata: Metadata): Boolean = {
      metadata.getEncoding == ParquetEncoding
    }
}

object ParquetStorageFormat {

  val ParquetEncoding = "parquet"
  val ParquetFileExtension = "parquet"
  val ParquetCompressionOpt = "parquet.compression"

}

class ParquetStorageFormat(fs: FileSystem,
                           root: Path,
                           val metadata: Metadata,
                           conf: Configuration) extends StorageFormat with LazyLogging {

  import ParquetStorageFormat._

  val sft: SimpleFeatureType = metadata.getSimpleFeatureType
  val scheme: PartitionScheme = metadata.getPartitionScheme
  val typeName: String = sft.getTypeName

  override def getFileExtension: String = ParquetFileExtension

  override def getMetadata: Metadata = metadata

  // TODO ask the partition manager the geometry is fully covered?
  override def getPartitionReader(q: Query, partition: Partition): FileSystemPartitionIterator = {

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
    val iters = getPaths(partition).toIterator.map { path =>
      if (!fs.exists(path)) {
        new EmptyFsIterator(partition)
      }
      else {
        val support = new SimpleFeatureReadSupport
        SimpleFeatureReadSupport.setSft(transformSft, conf)

        val builder = ParquetReader.builder[SimpleFeature](support, path)
          .withFilter(parquetFilter)
          .withConf(conf)

        new FilteringIterator(partition, builder, fc._2)
      }
    }
    new MultiIterator(partition, iters)
  }

  override def getWriter(partition: Partition): FileSystemWriter = {
    new FileSystemWriter {
      private val leaf     = scheme.isLeafStorage
      private val dataPath = StorageUtils.nextFile(fs, root, partition.getName, leaf, ParquetFileExtension)
      metadata.addFile(partition.getName, dataPath.getName)

      private val sftConf = {
        val c = new Configuration(conf)
        SimpleFeatureReadSupport.setSft(sft, c)
        c
      }

      private val writer = SimpleFeatureParquetWriter.builder(dataPath, sftConf).build()

      override def write(f: SimpleFeature): Unit = writer.write(f)

      override def flush(): Unit = {}

      override def close(): Unit = CloseQuietly(writer)
    }
  }

  override def getPaths(partition: Partition): java.util.List[Path] = {
    val baseDir = if (scheme.isLeafStorage) {
      StorageUtils.partitionPath(root, partition.getName).getParent
    } else {
      StorageUtils.partitionPath(root, partition.getName)
    }
    val files = metadata.getFiles(partition.getName)
    files.map(new Path(baseDir, _))
  }

  override def rawListPartitions(): util.List[Partition] =
    StorageUtils.buildPartitionList(fs, root, scheme, StorageUtils.SequenceLength, ParquetFileExtension)
      .map(new StoragePartition(_))

  override def rawListPaths(partition: Partition): util.List[Path] =
    StorageUtils.listStorageFiles(fs, root, partition, scheme.isLeafStorage, ParquetFileExtension)
}