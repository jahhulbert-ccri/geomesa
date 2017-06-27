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
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class FileSystemFeatureIterator(fs: FileSystem,
                                partitionScheme: PartitionScheme,
                                sft: SimpleFeatureType,
                                q: Query,
                                readThreads: Int,
                                storage: FileSystemStorage) extends java.util.Iterator[SimpleFeature] with AutoCloseable {
  // TODO: don't list partitions as there could be too many
  import scala.collection.JavaConversions._

  private val partitions: Seq[Partition] = {
    // Get the partitions from the partition scheme
    // if the result is empty, then scan all partitions
    // TODO: can we short-circuit if the query is outside the bounds
    val coveringPartitions = partitionScheme.getCoveringPartitions(q.getFilter).map(storage.getPartition)
    val storagePartitions = storage.listPartitions(sft.getTypeName)
    if (coveringPartitions.isEmpty) {
      storagePartitions
    } else {
      // TODO: for now we intersect the two to find the real files and not waste too much time
      coveringPartitions.intersect(storagePartitions)
    }
  }

//  private val threadedReader = new ThreadedReader(partitions.map(storage.getPartitionReader(q,_)), readThreads)
  private val threadedReader = new ThreadedReader(storage, partitions, q, readThreads)
  override def hasNext: Boolean = threadedReader.hasNext
  override def next(): SimpleFeature = threadedReader.next()
  override def close(): Unit = threadedReader.close()
}


//class ThreadedReader(readers: Seq[FileSystemPartitionIterator], numThreads: Int)
class ThreadedReader(storage: FileSystemStorage, partitions: Seq[Partition], q: Query, numThreads: Int)
  extends java.util.Iterator[SimpleFeature] with AutoCloseable with LazyLogging {

  // Need to do more tuning here. On a local system 1 thread (so basic producer/consumer) was best
  // because Parquet is also threading the reads underneath i think. using prod/cons pattern was
  // about 30% faster but increasing beyond 1 thread slowed things down. This could be due to the
  // cost of serializing simple features though. need to investigate more.
  //
  // However, if you are doing lots of filtering it appears that bumping the threads up high
  // can be very useful. Seems possibly numcores/2 might is a good setting (which is a standard idea)

  logger.info(s"Threadeding the read of ${partitions.size} partitions with $numThreads reader threads (and 1 writer thread)")
  private val es = Executors.newFixedThreadPool(numThreads)
  private val latch = new CountDownLatch(partitions.size)

  private val queue = new LinkedBlockingQueue[SimpleFeature](100000)

  private var started: Boolean = false
  private def start(): Unit = {
    partitions.foreach { p =>
      es.submit(new Runnable with LazyLogging {
        override def run(): Unit = {
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
              e.printStackTrace()
              logger.error(s"Error in reader for partition ${reader.getPartition}: ${e.getMessage}", e)
          } finally {
            latch.countDown()
            try { reader.close() } catch { case e: Exception => }
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
    latch.await(10, TimeUnit.SECONDS)
  }
}