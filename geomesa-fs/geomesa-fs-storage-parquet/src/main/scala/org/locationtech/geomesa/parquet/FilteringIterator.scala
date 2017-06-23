/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.apache.parquet.hadoop.ParquetReader
import org.locationtech.geomesa.fs.storage.api.{FileSystemPartitionIterator, Partition}
import org.opengis.feature.simple.SimpleFeature

/**
  * Created by ahulbert on 6/21/17.
  */
class FilteringIterator(partition: Partition,
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

  override def getPartition: Partition = partition
}


//class FilteringParquetPartitionIterator(partition: Partition,
//                                        sft: SimpleFeatureType,
//                                        query: org.geotools.data.Query) extends LazyLogging {
//  import org.locationtech.geomesa.index.conf.QueryHints._
//  private val transformSft = query.getHints.getTransformSchema.getOrElse(sft)
//
//  val support = new SimpleFeatureReadSupport(transformSft)
//  // TODO: push down predicates and partition pruning
//  // TODO ensure that transforms are pushed to the ColumnIO in parquet.
//  // TODO: Push down full filter that can't be managed
//  val fc = new FilterConverter(transformSft).convert(query.getFilter)
//  val parquetFilter =
//    fc._1
//      .map(FilterCompat.get)
//      .getOrElse(FilterCompat.NOOP)
//
//  logger.info(s"Parquet filter: $parquetFilter and modified gt filter ${fc._2}")
//
//  val reader = ParquetReader.builder[SimpleFeature](support, path)
//    .withFilter(parquetFilter)
//    .build()
//}

class FilteringParquetIterator(reader: ParquetReader[SimpleFeature],
                               gtFilter: org.opengis.filter.Filter) extends Iterator[SimpleFeature] {
  private var staged: SimpleFeature = _

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

}


class EmptyFsIterator(partition: Partition) extends FileSystemPartitionIterator {
  override def close(): Unit = {}
  override def next(): SimpleFeature = throw new NoSuchElementException
  override def hasNext: Boolean = false
  override def getPartition: Partition = partition
}