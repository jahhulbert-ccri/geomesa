/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, Partition}
import org.opengis.feature.simple.SimpleFeatureType

object PartitionUtils {

  // TODO: don't list storage partitions as there could be too many
  def getPartitionsForQuery(storage: FileSystemStorage,
                            sft: SimpleFeatureType,
                            q: Query): Seq[Partition] = {
    import scala.collection.JavaConversions._
    // Get the partitions from the partition scheme
    // if the result is empty, then scan all partitions
    // TODO: can we short-circuit if the query is outside the bounds
    val partitionScheme = storage.getPartitionScheme(sft)
    val coveringPartitions = partitionScheme.getCoveringPartitions(q.getFilter).map(storage.getPartition)
    val storagePartitions = storage.listPartitions(sft.getTypeName)
    if (coveringPartitions.isEmpty) {
      storagePartitions
    } else {
      // TODO: for now we intersect the two to find the real files and not waste too much time
      coveringPartitions.intersect(storagePartitions)
    }
  }
}
