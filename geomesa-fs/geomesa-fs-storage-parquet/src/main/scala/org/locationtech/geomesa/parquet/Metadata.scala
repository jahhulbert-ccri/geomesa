/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by ahulbert on 6/30/17.
  */
class Metadata(path: Path, conf: Configuration) extends LazyLogging {

  private val fs = path.getFileSystem(conf)
  private var cached: List[String] = read()

  private def read(): List[String] = {
    if (fs.exists(path)) {
      val in = path.getFileSystem(conf).open(path)
      try {
        import scala.collection.JavaConversions._
        IOUtils.readLines(in).toList
      } finally {
        in.close()
      }
    } else List.empty[String]
  }

  def add(toAdd: Seq[String]): Unit = {
    val parts = (read() ++ toAdd).distinct
    val out = path.getFileSystem(conf).create(path, true)
    parts.foreach{ p => out.writeBytes(p); out.write('\n') }
    out.hflush()
    out.hsync()
    out.close()
    cached = read()
    logger.info(s"wrote ${parts.size} partitions to metadata file")
  }

  def getPartitions: Seq[String] = {
    cached
  }
}
