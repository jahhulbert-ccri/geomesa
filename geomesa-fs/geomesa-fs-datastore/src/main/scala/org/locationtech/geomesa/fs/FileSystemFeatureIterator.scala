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
import org.locationtech.geomesa.fs.storage.api.{FileSystemPartitionIterator, FileSystemStorage}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class FileSystemFeatureIterator(fs: FileSystem,
                                partitionScheme: PartitionScheme,
                                sft: SimpleFeatureType,
                                q: Query,
                                storage: FileSystemStorage) extends java.util.Iterator[SimpleFeature] with AutoCloseable {
  // TODO: don't list partitions as there could be too many
  import scala.collection.JavaConversions._

  private val partitions: Seq[String] = {
      // Get the partitions from the partition scheme
    // if the result is empty, then scan all partitions
    // TODO: can we short-circuit if the query is outside the bounds
    val res = partitionScheme.coveringPartitions(q.getFilter)
    val storagePartitions = storage.listPartitions(sft.getTypeName)
    if (res.isEmpty) {
      storagePartitions
    } else {
      // TODO: for now we intersect the two to find the real files and not waste too much time
      res.intersect(storagePartitions)
    }
  }

//   private val readers = partitions.map(storage.getPartitionReader(q,_))
//   private val internal = readers.flatten.iterator
//  override def hasNext: Boolean = internal.hasNext
//  override def next(): SimpleFeature = internal.next()
//  override def close(): Unit = readers.foreach(_.close())

  private val threadedReader = new ThreadedReader(partitions.map(storage.getPartitionReader(q,_)))
  override def hasNext: Boolean = threadedReader.hasNext
  override def next(): SimpleFeature = threadedReader.next()
  override def close(): Unit = threadedReader.close()
}

class ThreadedReader(readers: Seq[FileSystemPartitionIterator])
  extends java.util.Iterator[SimpleFeature] with AutoCloseable with LazyLogging {

  // Need to do more tuning here. On a local system 1 thread (so basic producer/consumer) was best
  // because Parquet is also threading the reads underneath i think. using prod/cons pattern was
  // about 30% faster but increasing beyond 1 thread slowed things down. This could be due to the
  // cost of serializing simple features though. need to investigate more.
  private val numThreads = Option(System.getProperty("geomesa.fs.threadedreader.threads")).getOrElse("1").toInt
  logger.info(s"Using $numThreads")
  private val es = Executors.newFixedThreadPool(numThreads)
  private val latch = new CountDownLatch(readers.size)

  private val queue = new LinkedBlockingQueue[SimpleFeature](100000)

  private var started: Boolean = false
  private def start(): Unit = {
    readers.foreach { reader =>
      es.submit(new Runnable with LazyLogging {
        override def run(): Unit = {
          var count = 0
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
            latch.countDown()
            logger.info(s"Partition ${reader.getPartition} produced $count records")
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
    latch.await(5, TimeUnit.SECONDS)
    readers.foreach(_.close())
  }
}