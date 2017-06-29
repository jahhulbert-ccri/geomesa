/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File
import java.lang.Iterable

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.parquet.{SimpleFeatureReadSupport, SimpleFeatureWriteSupport}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.ConverterIngestJob
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable

class ParquetConverterJob(sft: SimpleFeatureType,
                          converterConfig: Config,
                          dsPath: Path,
                          tempPath: Option[Path],
                          reducers: Int) extends ConverterIngestJob(sft, converterConfig) with LazyLogging {

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
    job.setNumReduceTasks(reducers)
    job.setReducerClass(classOf[DummyReducer])
    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")

    // Ensure that the reducers don't start to early (default is at 0.05 which takes all the map slots and isn't needed)
    job.getConfiguration.set("mapreduce.job.reduce.slowstart.completedmaps", ".90")

    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

    // Output format
    job.setOutputFormatClass(classOf[SchemeOutputFormat])
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[SimpleFeature])

    // Super important
    job.getConfiguration.set(ParquetOutputFormat.JOB_SUMMARY_LEVEL, ParquetOutputFormat.JobSummaryLevel.NONE.toString)

    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[SimpleFeatureWriteSupport])
    ParquetConverterJob.setSimpleFeatureType(job.getConfiguration, sft)

    FileOutputFormat.setOutputPath(job, tempPath.getOrElse(dsPath))

    FileInputFormat.setInputPaths(job, paths.mkString(","))
    configureJob(job)

    Command.user.info("Submitting job - please wait...")
    job.submit()
    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    while (!job.isComplete) {
      if (job.getStatus.getState != JobStatus.State.PREP) {
        statusCallback(job.mapProgress(), written(job), failed(job), false)
      }
      Thread.sleep(1000)
    }
    statusCallback(job.mapProgress(), written(job), failed(job), true)

    // Do this earlier than the data copy bc its throwing errors and shit
    val res = (written(job), failed(job))

    if (job.isSuccessful) {
      if (tempPath.isDefined) {
        distCopy(tempPath.get, dsPath, sft, job.getConfiguration)
      }
    } else {
      Command.user.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
    }

    res
  }

  def distCopy(srcRoot: Path, dest: Path, sft: SimpleFeatureType, conf: Configuration): Boolean = {
    val typeName = sft.getTypeName
    val typePath = new Path(srcRoot, typeName)
    val destTypePath = new Path(dest, typeName)

    Command.user.info(s"Attempting to distcp $typePath to $destTypePath")

    val opts = new DistCpOptions(List(typePath), destTypePath)
    opts.setAppend(false)
    opts.setOverwrite(true)
    val job = new DistCp(new Configuration, opts).execute()

    val success = job.waitForCompletion(true)
    if (success) {
      Command.user.info(s"Successfully copied data to $dest")
    } else {
      Command.user.error(s"failed to copy data to $dest")
    }
    success
  }

  // TODO probably make a better method for this and extract it to a static utility class
  // TODO parallelize if the filesystems are not the same
  def copyData(srcRoot: Path, destRoot: Path, sft: SimpleFeatureType, conf: Configuration): Boolean = {
    val typeName = sft.getTypeName
    Command.user.info(s"Job finished...copying data from $srcRoot to $destRoot for type $typeName")

    val srcFS = srcRoot.getFileSystem(conf)
    val destFS = destRoot.getFileSystem(conf)

    val typePath = new Path(srcRoot, typeName)
    val foundFiles = srcFS.listFiles(typePath, true)

    val storageFiles = mutable.ListBuffer.empty[Path]
    while (foundFiles.hasNext) {
      val f = foundFiles.next()
      if (!f.isDirectory) {
        storageFiles += f.getPath
      }
    }

    storageFiles.forall { f =>
      val child = f.toString.replace(srcRoot.toString, "")
      val target = new Path(destRoot, if (child.startsWith("/")) child.drop(1) else child)
      logger.info(s"Moving $f to $target")
      if (!destFS.exists(target.getParent)) {
        destFS.mkdirs(target.getParent)
      }
      FileUtil.copy(srcFS, f, destFS, target, true, true, conf)
    }
  }

}

object ParquetConverterJob {
  def setSimpleFeatureType(conf: Configuration, sft: SimpleFeatureType): Unit = {
    // Validate that there is a partition scheme
    org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(sft)
    SimpleFeatureReadSupport.updateConf(sft, conf)
  }

  def getSimpleFeatureType(conf: Configuration): SimpleFeatureType = {
    SimpleFeatureReadSupport.sftFromConf(conf)
  }
}

class IngestMapper extends Mapper[LongWritable, SimpleFeature, Text, BytesWritable] with LazyLogging {

  type Context = Mapper[LongWritable, SimpleFeature, Text, BytesWritable]#Context

  private var serializer: KryoFeatureSerializer = _
  private var partitionScheme: PartitionScheme = _

  var written: Counter = _

  override def setup(context: Context): Unit = {
    super.setup(context)
    val sft = ParquetConverterJob.getSimpleFeatureType(context.getConfiguration)
    serializer = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)
    partitionScheme = org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(sft)

    written = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written)
  }

  override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
    logger.debug(s"map key ${key.toString}, map value ${DataUtilities.encodeFeature(sf)}")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

    context.getCounter("geomesa", "map").increment(1)
    // partitionKey is important because this needs to be correct for the parquet file
    val partitionKey = new Text(partitionScheme.getPartitionName(sf))
    context.write(partitionKey, new BytesWritable(serializer.serialize(sf)))
    written.increment(1)
  }
}

class DummyReducer extends Reducer[Text, BytesWritable, Void, SimpleFeature] {

  type Context = Reducer[Text, BytesWritable, Void, SimpleFeature]#Context

  private var serializer: KryoFeatureSerializer = _
  var reduced: Counter = _

  override def setup(context: Context): Unit = {
    super.setup(context)
    val sft = ParquetConverterJob.getSimpleFeatureType(context.getConfiguration)
    serializer = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)
    reduced = context.getCounter(GeoMesaOutputFormat.Counters.Group, "reduced")
  }

  override def reduce(key: Text, values: Iterable[BytesWritable], context: Context): Unit = {
    values.foreach { bw =>
      context.write(null, serializer.deserialize(bw.getBytes))
      reduced.increment(1)
    }
  }

}

class SchemeOutputFormat extends ParquetOutputFormat[SimpleFeature] {
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, SimpleFeature] = {

    val sft = ParquetConverterJob.getSimpleFeatureType(context.getConfiguration)
    val name = sft.getTypeName

    new RecordWriter[Void, SimpleFeature] with LazyLogging {

      private val partitionScheme = org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(sft)

      var curPartition: String = _
      var writer: RecordWriter[Void, SimpleFeature] = _
      var sentToParquet: Counter = context.getCounter(GeoMesaOutputFormat.Counters.Group, "sentToParquet")

      override def write(key: Void, value: SimpleFeature): Unit = {
        val basePath = name + "/" + partitionScheme.getPartitionName(value)         // TODO once this is done we need to fix up these file names to do parts or something?

        def initWriter() = {
          val extension = ".parquet" // TODO this has to match the FS from the geomesa-fs abstraction need to do that
          val committer = getOutputCommitter(context).asInstanceOf[FileOutputCommitter]
          val file = new Path(committer.getWorkPath, basePath + extension)
          logger.info(s"Creating Date scheme record writer at path ${file.toString}")
          curPartition = basePath
          writer = getRecordWriter(context, file)
        }

        if (writer == null) {
          initWriter()
        } else if (basePath != curPartition) {
          writer.close(context)
          logger.info(s"Closing writer for $curPartition")
          initWriter()
        }
        writer.write(key, value)
        sentToParquet.increment(1)
      }

      override def close(context: TaskAttemptContext): Unit = {
        if (writer != null) writer.close(context)
      }
    }
  }

  override def checkOutputSpecs(job: JobContext): Unit = {
    // Ensure that the output directory is set and not already there
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) throw new InvalidJobConfException("Output directory not set.")
    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }
}