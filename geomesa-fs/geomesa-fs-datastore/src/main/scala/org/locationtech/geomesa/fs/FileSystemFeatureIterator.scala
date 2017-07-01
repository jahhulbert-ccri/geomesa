/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs

import java.util.concurrent.{CountDownLatch, Executors, LinkedBlockingQueue, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.FileSystem
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, Partition, PartitionScheme}
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class FileSystemFeatureIterator(fs: FileSystem,
                                partitionScheme: PartitionScheme,
                                sft: SimpleFeatureType,
                                q: Query,
                                readThreads: Int,
                                storage: FileSystemStorage) extends java.util.Iterator[SimpleFeature] with AutoCloseable with LazyLogging {

  private val partitions = PartitionUtils.getPartitionsForQuery(storage, sft, q)

  private val iter: java.util.Iterator[SimpleFeature] with AutoCloseable =
    if (partitions.isEmpty) {
      new java.util.Iterator[SimpleFeature] with AutoCloseable {
        override def next(): SimpleFeature = throw new NoSuchElementException
        override def hasNext: Boolean = false
        override def close(): Unit = {}
      }
    } else {
      new ThreadedReader(storage, partitions, q, readThreads)
    }

  override def hasNext: Boolean = iter.hasNext
  override def next(): SimpleFeature = iter.next()
  override def close(): Unit = iter.close()
}

class ThreadedReader(storage: FileSystemStorage, partitions: Seq[Partition], q: Query, numThreads: Int)
  extends java.util.Iterator[SimpleFeature] with AutoCloseable with LazyLogging {

  assert(partitions.nonEmpty, "Partitions is empty")
  logger.info(s"Threading the read of ${partitions.size} partitions with $numThreads reader threads (and 1 writer thread)")

  // Need to do more tuning here. On a local system 1 thread (so basic producer/consumer) was best
  // because Parquet is also threading the reads underneath i think. using prod/cons pattern was
  // about 30% faster but increasing beyond 1 thread slowed things down. This could be due to the
  // cost of serializing simple features though. need to investigate more.
  //
  // However, if you are doing lots of filtering it appears that bumping the threads up high
  // can be very useful. Seems possibly numcores/2 might is a good setting (which is a standard idea)

  private val es = Executors.newFixedThreadPool(numThreads)
  private val latch = new CountDownLatch(partitions.size)

  private val queue = new LinkedBlockingQueue[SimpleFeature](100000)

  private var started: Boolean = false
  private def start(): Unit = {
    partitions.foreach { p =>
      es.submit(new Runnable with LazyLogging {
        override def run(): Unit = {
          try {
            var count = 0
            val reader = storage.getPartitionReader(q, p)
            try {
              while (reader.hasNext) {
                count += 1
                val next = reader.next()
                while (!queue.offer(next, 3, TimeUnit.MILLISECONDS)) {}
              }
            } catch {
              case e: Throwable =>
                logger.error(s"Error in reader for partition ${reader.getPartition}: ${e.getMessage}", e)
            } finally {
              CloseQuietly(reader)
              logger.info(s"Partition ${reader.getPartition} produced $count records")
            }
          } finally {
            latch.countDown()
          }
        }
      })
    }
    es.shutdown()
    started = true
  }

  private var nextQueued: Boolean = false
  private var cur: SimpleFeature = _

  private def queueNext(): Unit = {
    while (cur == null && (queue.size() > 0 || latch.getCount > 0)) {
      cur = queue.poll(5, TimeUnit.MILLISECONDS)
    }
    nextQueued = true
  }

  override def next(): SimpleFeature = {
    if (!nextQueued) { queueNext() }
    if (cur == null) throw new NoSuchElementException

    val ret = cur
    cur = null
    nextQueued = false

    ret
  }

  override def hasNext: Boolean = {
    if (!started) start()
    if (!nextQueued) { queueNext() }
    cur != null
  }

  override def close(): Unit = {
    es.awaitTermination(5, TimeUnit.SECONDS)
    latch.await(10, TimeUnit.SECONDS)
  }
}