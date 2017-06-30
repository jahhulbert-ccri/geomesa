/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.util.concurrent.atomic.AtomicLong

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalListener, RemovalNotification}
import org.apache.hadoop.fs.FileSystem
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, FileSystemWriter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable

class FileSystemFeatureStore(entry: ContentEntry,
                             query: Query,
                             fs: FileSystem,
                             storage: FileSystemStorage,
                             readThreads: Int) extends ContentFeatureStore(entry, query) {
  private val _sft = storage.getFeatureType(entry.getTypeName)

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    require(flags != 0, "no write flags set")
    require((flags | WRITER_ADD) == WRITER_ADD, "Only append supported")

    new FeatureWriter[SimpleFeatureType, SimpleFeature] {
      private val typeName = query.getTypeName
      private val rl = new RemovalListener[String, FileSystemWriter] {
        override def onRemoval(removalNotification: RemovalNotification[String, FileSystemWriter]): Unit = {
          val writer = removalNotification.getValue
          writer.flush()
          writer.close()
        }
      }
      private val loader = new CacheLoader[String, FileSystemWriter] {
        override def load(k: String): FileSystemWriter = storage.getWriter(typeName, storage.getPartition(k))
      }
      private val writers =
        CacheBuilder.newBuilder()
          .removalListener(rl)
          .build[String, FileSystemWriter](loader)

      private val partitionsWritten = mutable.HashSet.empty[String]

      private val sft = _sft

      // TODO: figure out flushCount
      private val flushCount = 1000
      private val featureIds = new AtomicLong(0)
      private var count = 0L
      private var feature: SimpleFeature = _

      override def getFeatureType: SimpleFeatureType = sft

      override def hasNext: Boolean = false

      override def next(): SimpleFeature = {
        feature = new ScalaSimpleFeature(featureIds.getAndIncrement().toString, sft)
        feature
      }

      override def write(): Unit = {
        val partition = storage.getPartitionScheme(typeName).getPartitionName(feature)
        partitionsWritten += partition
        val writer = writers.get(partition)
        writer.write(feature)

        feature = null
        count += 1
//        if (count % flushCount == 0) {
//          writer.flush()
//        }
      }

      override def remove(): Unit = throw new NotImplementedError()

      override def close(): Unit = {
        writers.invalidateAll()
        import scala.collection.JavaConversions._
        storage.newPartitions(sft.getTypeName, partitionsWritten.toList)
      }

    }
  }

  override def getBoundsInternal(query: Query): ReferencedEnvelope = ReferencedEnvelope.EVERYTHING
  override def buildFeatureType(): SimpleFeatureType = _sft
  override def getCountInternal(query: Query): Int = ???
  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    // TODO the type name can sometimes be empty such as Query.ALL
    query.setTypeName(_sft.getTypeName)
    new DelegateSimpleFeatureReader(_sft,
      new DelegateSimpleFeatureIterator(
        new FileSystemFeatureIterator(fs, storage.getPartitionScheme(_sft.getTypeName), _sft, query, readThreads, storage)))
  }


  override def canLimit: Boolean = false
  override def canTransact: Boolean = false
  override def canEvent: Boolean = false
  override def canReproject: Boolean = false
  override def canRetype: Boolean = true
  override def canSort: Boolean = true
  override def canFilter: Boolean = true

}
