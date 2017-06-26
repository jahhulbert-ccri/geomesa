/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.{File, IOException}
import java.lang.Iterable
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.DateTimeZ2Scheme
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.mapreduce.FileStreamInputFormat
import org.locationtech.geomesa.parquet.SimpleFeatureWriteSupport
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.ConverterIngestJob
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by ahulbert on 6/26/17.
  */
class ParquetConverterJob(sft: SimpleFeatureType,
                          converterConfig: Config,
                          dsPath: Path,
                          tempPath: Path) extends ConverterIngestJob(sft, converterConfig) with LazyLogging {

  override def run(dsParams: Map[String, String],
    typeName: String,
    paths: Seq[String],
    libjarsFile: String,
    libjarsPaths: Iterator[() => Seq[File]],
    statusCallback: (Float, Long, Long, Boolean) => Unit = (_, _, _, _) => Unit): (Long, Long) = {

    val job = Job.getInstance(new Configuration, "GeoMesa Parquet Ingest")

    JobUtils.setLibJars(job.getConfiguration, readLibJars(libjarsFile), defaultSearchPath ++ libjarsPaths)

    job.setJarByClass(getClass)
    job.setMapperClass(classOf[IngestMapper])
    job.setInputFormatClass(inputFormatClass)
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[BytesWritable])

    // Dummy reducer to convert to void and shuffle
    job.setNumReduceTasks(1)
    job.setReducerClass(classOf[DummyReducer])
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")

    // Output format
    job.setOutputFormatClass(classOf[SchemeOutputFormat])
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[SimpleFeature])

    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[SimpleFeatureWriteSupport])
    job.getConfiguration.set("sft.name", sft.getTypeName)
    job.getConfiguration.set("sft.spec", SimpleFeatureTypes.encodeType(sft, true))
    dsPath.getFileSystem(job.getConfiguration)
    val temp = new Path(tempPath, "abcisrandom")
    FileOutputFormat.setOutputPath(job, temp)

    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

    FileInputFormat.setInputPaths(job, paths.mkString(","))
    configureJob(job)

    Command.user.info("Submitting job - please wait...")
    job.submit()
    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    while (!job.isComplete) {
      if (job.getStatus.getState != JobStatus.State.PREP) {
        statusCallback(job.mapProgress(), written(job), failed(job), false) // we don't have any reducers, just track mapper progress
      }
      Thread.sleep(1000)
    }
    statusCallback(job.mapProgress(), written(job), failed(job), true)

    if (!job.isSuccessful) {
      Command.user.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
    } else {
      if (!sync(new Path(temp, sft.getTypeName), new Path(dsPath, sft.getTypeName), job.getConfiguration)) {
        Command.user.error(s"Error syncing")
      }
    }

    (written(job), failed(job))
  }

  // TODO probably make a better method for this and extract it to a static utility class
  def sync(src: Path, dest : Path, conf: Configuration): Boolean = {
    logger.info(s"importing from ${src.toString} to  ${dest.toString} ")
    val srcPrefix = src.toString
    val fs = src.getFileSystem(conf)
    val t1 = fs.listFiles(src, true)
    val files = mutable.ListBuffer.empty[Path]
    while (t1.hasNext) {
      val t = t1.next()
      if (! t.isDirectory) {
        files += t.getPath
      }
    }
    files.foreach { f =>
      val child = f.toString.replace(srcPrefix, "")
      val target = new Path(dest, if (child.startsWith("/")) child.drop(1) else child)
      logger.info(s"Moving $f to $target")
      try {
        if (!fs.exists(target.getParent)) {
          logger.info(s"mkdir $target")
          fs.mkdirs(target.getParent)
        }
        if (!fs.rename(f, target)) throw new IOException(s"Unable to move $f to $target")
      } catch {
        case e: Exception =>
          logger.error(s" Error moving ${f.toString} to ${target.toString}", e)
          return false
      }
    }
    true
  }


}

class IngestMapper extends Mapper[LongWritable, SimpleFeature, Text, BytesWritable] with LazyLogging {

  type Context = Mapper[LongWritable, SimpleFeature, Text, BytesWritable]#Context

  private var serializer: KryoFeatureSerializer = _
  private var partitionScheme: PartitionScheme = _

  override def setup(context: Context): Unit = {
    super.setup(context)
    val spec = context.getConfiguration.get(FileStreamInputFormat.SftKey)
    val name = context.getConfiguration.get(FileStreamInputFormat.TypeNameKey)
    val sft = SimpleFeatureTypes.createType(name, spec)
    serializer = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)
    partitionScheme = new DateTimeZ2Scheme(DateTimeFormatter.ofPattern("yyyy/DDD/HH"), ChronoUnit.HOURS, 1, 10, sft, "dtg", "geom")
  }

  override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
    logger.debug(s"map key ${key.toString}, map value ${DataUtilities.encodeFeature(sf)}")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

    // partitionKey is important because this needs to be correct for the parquet file
    val partitionKey = new Text(partitionScheme.getPartitionName(sf))
    context.write(partitionKey, new BytesWritable(serializer.serialize(sf)))
  }
}

class DummyReducer extends Reducer[Text, BytesWritable, Void, SimpleFeature] {

  type Context = Reducer[Text, BytesWritable, Void, SimpleFeature]#Context

  private var serializer: KryoFeatureSerializer = _

  override def setup(context: Context): Unit = {
    super.setup(context)
    val spec = context.getConfiguration.get(FileStreamInputFormat.SftKey)
    val name = context.getConfiguration.get(FileStreamInputFormat.TypeNameKey)
    val sft = SimpleFeatureTypes.createType(name, spec)
    serializer = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)
  }

  override def reduce(key: Text, values: Iterable[BytesWritable], context: Context): Unit = {
    values.foreach { bw =>
      context.write(null, serializer.deserialize(bw.getBytes))
    }
  }

}

class SchemeOutputFormat extends ParquetOutputFormat[SimpleFeature] {
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, SimpleFeature] = {

    val spec = context.getConfiguration.get(FileStreamInputFormat.SftKey)
    val name = context.getConfiguration.get(FileStreamInputFormat.TypeNameKey)
    val sft = SimpleFeatureTypes.createType(name, spec)
    new RecordWriter[Void, SimpleFeature] with LazyLogging {

      private val partitionScheme = new DateTimeZ2Scheme(DateTimeFormatter.ofPattern("yyyy/DDD/HH"), ChronoUnit.HOURS, 1, 10, sft, "dtg", "geom")
      private val writers = mutable.HashMap.empty[String, RecordWriter[Void, SimpleFeature]]


      override def write(key: Void, value: SimpleFeature): Unit = {
        // TODO once this is done we need to fix up these file names to do parts or something?
        val basePath = name + "/" + partitionScheme.getPartitionName(value)
        if (writers.contains(basePath)) {
          writers(basePath).write(key, value)
        } else {
          val codec = CodecConfig.from(context).getCodec
          val extension = ".parquet"
          val committer = getOutputCommitter(context).asInstanceOf[FileOutputCommitter]
          val file = new Path(committer.getWorkPath, basePath + extension)
          logger.info(s"Creating Date scheme record writer at path ${file.toString}")
          val rr = getRecordWriter(context, file)
          writers(basePath) = rr
          rr.write(key, value)
        }
      }

      override def close(context: TaskAttemptContext): Unit = {
        writers.values.foreach(_.close(context))
      }
    }
  }
}