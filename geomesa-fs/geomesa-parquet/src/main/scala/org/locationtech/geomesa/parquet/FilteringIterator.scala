/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.apache.parquet.hadoop.ParquetReader
import org.locationtech.geomesa.fs.storage.api.FileSystemPartitionIterator
import org.opengis.feature.simple.SimpleFeature

/**
  * Created by ahulbert on 6/21/17.
  */
class FilteringIterator(partition: String,
                        reader: ParquetReader[SimpleFeature],
                        gtFilter: org.opengis.filter.Filter) extends FileSystemPartitionIterator {

  private var staged: SimpleFeature = _

  override def close(): Unit = reader.close()

  override def next(): SimpleFeature = staged

  override def hasNext: Boolean = {
    staged = null
    var cont = true
    while (staged == null && cont) {
      val f = reader.read()
      if (f == null) {
        cont = false
      } else if (gtFilter.evaluate(f)) {
        staged = f
      }
    }
    staged != null
  }

  override def getPartition: String = partition
}

class EmptyFsIterator(partition: String) extends FileSystemPartitionIterator {
  override def close(): Unit = {}
  override def next(): SimpleFeature = throw new NoSuchElementException
  override def hasNext: Boolean = false
  override def getPartition: String = partition
}