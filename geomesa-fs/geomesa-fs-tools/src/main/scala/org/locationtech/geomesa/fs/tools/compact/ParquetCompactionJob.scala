/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import java.io.{DataInput, DataOutput, File}
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text, Writable}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}
import org.geotools.data.{DataStoreFinder, DataUtilities, Query}
import org.geotools.factory.Hints
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.api.FileSystemPartitionIterator
import org.locationtech.geomesa.fs.storage.common.{FileType, StorageUtils}
import org.locationtech.geomesa.fs.tools.ingest.{ParquetJobUtils, SchemeOutputFormat}
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat.{Counters => OutCounters}
import org.locationtech.geomesa.parquet.{SimpleFeatureReadSupport, SimpleFeatureWriteSupport}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest.AbstractIngestJob
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

class ParquetCompactionJob(sft: SimpleFeatureType,
                           dsPath: Path,
                           tempPath: Option[Path]) extends AbstractIngestJob with LazyLogging {

  override def run(dsParams: Map[String, String],
                   typeName: String,
                   paths: Seq[String],
                   libjarsFile: String,
                   libjarsPaths: Iterator[() => Seq[File]],
                   statusCallback: StatusCallback): (Long, Long) = {

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[FileSystemDataStore]
    val job = Job.getInstance(new Configuration, "GeoMesa Parquet Compaction")

    JobUtils.setLibJars(job.getConfiguration, readLibJars(libjarsFile), defaultSearchPath ++ libjarsPaths)

    job.setJarByClass(getClass)

    // InputFormat and Mappers
    job.setMapperClass(classOf[CompactionMapper])
    job.setInputFormatClass(classOf[PartitionInputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[BytesWritable])
    ParquetInputFormat.setReadSupportClass(job, classOf[SimpleFeatureReadSupport])
    import org.locationtech.geomesa.fs.FileSystemDataStoreParams
    PartitionInputFormat.setFsPath(job.getConfiguration, FileSystemDataStoreParams.PathParam.lookUp(dsParams).asInstanceOf[String])
    PartitionInputFormat.setFsEncoding(job.getConfiguration, FileSystemDataStoreParams.EncodingParam.lookUp(dsParams).asInstanceOf[String])

    // No reducers - Mapper will read/write its own things
    job.setNumReduceTasks(0)

    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

    // Output format
    job.setOutputFormatClass(classOf[SchemeOutputFormat])
    SchemeOutputFormat.setFileType(job.getConfiguration, FileType.Compacted)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[SimpleFeature])

    // Parquet Options
    val summaryLevel = Option(sft.getUserData.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL).asInstanceOf[String])
      .getOrElse(ParquetOutputFormat.JobSummaryLevel.NONE.toString)
    job.getConfiguration.set(ParquetOutputFormat.JOB_SUMMARY_LEVEL, summaryLevel)
    Command.user.info(s"Parquet metadata summary level is $summaryLevel")

    val compression = Option(sft.getUserData.get(ParquetOutputFormat.COMPRESSION).asInstanceOf[String])
      .map(CompressionCodecName.valueOf)
      .getOrElse(CompressionCodecName.SNAPPY)
    ParquetOutputFormat.setCompression(job, compression)
    Command.user.info(s"Parquet compression is $compression")

    // More Parquet config
    ParquetOutputFormat.setWriteSupportClass(job, classOf[SimpleFeatureWriteSupport])
    ParquetJobUtils.setSimpleFeatureType(job.getConfiguration, sft)

    FileOutputFormat.setOutputPath(job, tempPath.getOrElse(dsPath))

    Command.user.info("Submitting job - please wait...")
    job.submit()
    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    def mapCounters = Seq(("mapped", written(job)), ("failed", failed(job)))

    val stageCount = if (tempPath.isDefined) { "2" } else { "1" }

    while (!job.isComplete) {
      if (job.getStatus.getState != JobStatus.State.PREP) {
        val mapProgress = job.mapProgress()
        if (mapProgress < 1f) {
          statusCallback(s"Map (stage 1/$stageCount): ", mapProgress, mapCounters, done = false)
        } else {
          statusCallback(s"Map (stage 1/$stageCount): ", mapProgress, mapCounters, done = true)
          statusCallback.reset()
        }
      }
      Thread.sleep(1000)
    }

    val res = (written(job), failed(job))

    val ret = job.isSuccessful &&
      tempPath.forall(tp => ParquetJobUtils.distCopy(tp, dsPath, sft, job.getConfiguration, statusCallback)) && {
      Command.user.info("Attempting to update metadata")
      // We sleep here to allow a chance for S3 to become "consistent" with its storage listings
      Thread.sleep(5000)
      ds.storage.updateMetadata(typeName)
      Command.user.info("Metadata Updated")
      true
    }

    if (!ret) {
      Command.user.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
    }

    res
  }

  override def inputFormatClass: Class[_ <: FileInputFormat[_, SimpleFeature]] = null

  override def written(job: Job): Long =
    job.getCounters.findCounter(OutCounters.Group, OutCounters.Written).getValue

  override def failed(job: Job): Long =
    job.getCounters.findCounter(OutCounters.Group, OutCounters.Failed).getValue

  override def configureJob(job: Job): Unit = {}
}


import scala.collection.JavaConversions._

class PartitionInputSplit() extends InputSplit with Writable {
  private var name: String = _
  private var length: Long = _

  def getName: String = name
  override def getLength: Long = length
  override def getLocations: Array[String] = Array.empty[String]

  override def write(out: DataOutput): Unit = {
    out.writeUTF(name)
    out.writeLong(length)
  }

  override def readFields(in: DataInput): Unit = {
    this.name = in.readUTF()
    this.length = in.readLong()
  }
}

object PartitionInputSplit{
  def apply(name: String, length: Long): PartitionInputSplit = {
    val split = new PartitionInputSplit
    split.name = name
    split.length = length
    split
  }
}

class PartitionInputFormat extends InputFormat[Void, SimpleFeature] {

  override def getSplits(context: JobContext): util.List[InputSplit] = {
    val rootPath = PartitionInputFormat.getFsPath(context.getConfiguration)
    val encoding = PartitionInputFormat.getFsEncoding(context.getConfiguration)
    val dsParams = Map(
      "fs.path" -> rootPath,
      "fs.encoding" -> encoding
    )
    val ds: FileSystemDataStore = DataStoreFinder.getDataStore(dsParams).asInstanceOf[FileSystemDataStore]
    val typeName: String = ParquetJobUtils.getSimpleFeatureType(context.getConfiguration).getTypeName
    val metadata = ds.storage.getMetadata(typeName)
    val fs = ds.root.getFileSystem(context.getConfiguration)

    val splits = metadata.getPartitions.map { p =>
      val pp = StorageUtils.partitionPath(new Path(rootPath), typeName, p)
      val size = StorageUtils.listFileStatuses(fs, pp, "parquet").map(_.getLen).sum
      PartitionInputSplit(p, size)
    }

    splits
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Void, SimpleFeature] = {
    val partitionInputSplit = split.asInstanceOf[PartitionInputSplit]
    new RecordReader[Void, SimpleFeature] {
      private var sft: SimpleFeatureType = _
      private var reader: FileSystemPartitionIterator = _

      private var curValue: SimpleFeature = _

      override def getProgress: Float = 0.0f

      override def nextKeyValue(): Boolean = {
        curValue = if (reader.hasNext) reader.next() else null
        curValue != null
      }

      override def getCurrentValue: SimpleFeature = curValue

      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
        sft = ParquetJobUtils.getSimpleFeatureType(context.getConfiguration)

        val path = PartitionInputFormat.getFsPath(context.getConfiguration)
        val encoding = PartitionInputFormat.getFsEncoding(context.getConfiguration)
        val dsParams = Map(
          "fs.path" -> path,
          "fs.encoding" -> encoding
        )
        val ds: FileSystemDataStore = DataStoreFinder.getDataStore(dsParams).asInstanceOf[FileSystemDataStore]

        reader = ds.storage.getPartitionReader(sft, new Query(sft.getTypeName, Filter.INCLUDE), partitionInputSplit.getName)
      }

      override def getCurrentKey: Void = null

      override def close(): Unit = reader.close()
    }
  }
}

object PartitionInputFormat {
  val FsPathParam = "geomesa.fs.path"
  val FsEncodingParam = "geomesa.fs.encoding"

  def setFsPath(conf: Configuration, path: String): Unit = conf.set(FsPathParam, path)
  def getFsPath(conf: Configuration): String = conf.get(FsPathParam)

  def setFsEncoding(conf: Configuration, encoding: String): Unit = conf.set(FsEncodingParam, encoding)
  def getFsEncoding(conf: Configuration): String = conf.get(FsEncodingParam)

}

class CompactionMapper extends Mapper[LongWritable, SimpleFeature, Void, SimpleFeature] with LazyLogging {

  type Context = Mapper[LongWritable, SimpleFeature, Void, SimpleFeature]#Context

  var written: Counter = _
  var failed: Counter = _

  override def setup(context: Context): Unit = {
    super.setup(context)
    written = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written)
    failed = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Failed)
  }

  override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    context.getCounter("geomesa", "map").increment(1)
    try {
      context.write(null, sf)
      written.increment(1)
    } catch {
      case e: Throwable =>
        logger.error(s"Failed to write '${DataUtilities.encodeFeature(sf)}'", e)
        failed.increment(1)
    }
  }
}

