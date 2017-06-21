/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.converter

import java.io.FileInputStream

import org.locationtech.geomesa.convert.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api.FileSystemPartitionIterator
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}


class ConverterPartitionReader(partition: String,
                                 sft: SimpleFeatureType,
                                 converter: SimpleFeatureConverter[_],
                                 gtFilter: org.opengis.filter.Filter) extends FileSystemPartitionIterator {

  private val fis = new FileInputStream(partition)
  private val iter = converter.process(fis)

  private var cur: SimpleFeature = _

  private var nextStaged: Boolean = false
  private def stageNext() = {
    while (cur == null && iter.hasNext) {
      val possible = iter.next()
      if (gtFilter.evaluate(possible)) {
        cur = possible
      }
    }
    nextStaged = true
  }

  override def close(): Unit = {
    fis.close()
    converter.close()
  }

  override def next(): SimpleFeature = {
    if (!nextStaged) {
      stageNext()
    }

    if (cur == null) throw new NoSuchElementException

    val ret = cur
    cur = null
    nextStaged = false
    ret
  }

  override def hasNext: Boolean = {
    if (!nextStaged) {
      stageNext()
    }

    cur != null
  }

  override def getPartition: String = partition
}