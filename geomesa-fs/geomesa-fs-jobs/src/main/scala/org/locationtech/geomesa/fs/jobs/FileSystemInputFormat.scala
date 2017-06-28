/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.opengis.feature.simple.SimpleFeature

/**
  * Created by ahulbert on 6/28/17.
  */
class FileSystemInputFormat extends FileInputFormat[LongWritable, SimpleFeature] {

  type SFRR = RecordReader[LongWritable, SimpleFeature]

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): SFRR = {
    split.getLength
  }

}

object FileSystemInputFormat {

}

class FSRecordReader extends RecordReader[LongWritable, SimpleFeature] with LazyLogging {
  override def getProgress: Float = ???

  override def nextKeyValue(): Boolean = ???

  override def getCurrentValue: SimpleFeature = ???

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = ???

  override def getCurrentKey: LongWritable = ???

  override def close(): Unit = ???
}
