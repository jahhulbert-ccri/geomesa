/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.Parameters
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams}
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

import scala.collection.JavaConversions._

class CompactCommand extends FsDataStoreCommand with LazyLogging {

  override val name: String = "compact"
  override val params = new CompactParams

  override def execute(): Unit = {
    withDataStore(compact)
  }

  def compact(ds: FileSystemDataStore): Unit = {
    Command.user.info(s"Beginning Compaction Process...updating metadata")

    ds.storage.updateMetadata(params.featureName)
    val m = ds.storage.getMetadata(params.featureName)
    val toCompact = m.getPartitions.map(p => (p, m.getFiles(p))).filter(_._2.size() > 1).map(_._1)
    Command.user.info(s"Metadata update complete...compacting ${toCompact.size} partitions")

    toCompact.foreach { p =>
      logger.info(s"Compacting ${params.featureName}:$p")
      ds.storage.compact(params.featureName, p)
      logger.info(s"Completed compaction of ${params.featureName}:$p")
    }
    Command.user.info(s"Compaction completed")
  }
}

@Parameters(commandDescription = "Generate the metadata file")
class CompactParams extends FsParams with RequiredTypeNameParam
