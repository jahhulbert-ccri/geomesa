/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams}
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import scala.collection.JavaConversions._

// TODO we need multi threaded ingest for this
class FsIngestCommand extends IngestCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params = new FsIngestParams

  override val libjarsFile: String = "org/locationtech/geomesa/fs/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_FS_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[FileSystemDataStore])
  )

  override def execute(): Unit = {

    // validate arguments
    if (params.config == null) {
      throw new ParameterException("Converter Config argument is required")
    }
    if (params.spec == null) {
      throw new ParameterException("SimpleFeatureType specification argument is required")
    }

    val sft = CLArgResolver.getSft(params.spec, params.featureName)
    val converterConfig = CLArgResolver.getConfig(params.config)
    val ingest =
      new ParquetConverterIngest(sft,
        connection,
        converterConfig,
        params.files,
        libjarsFile,
        libjarsPaths,
        params.threads,
        new Path(params.path),
        new Path(params.tempDir))
    ingest.run()
  }

}

// TODO implement datetime format, etc here for ingest or create type maybe?
@Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
class FsIngestParams extends IngestParams with FsParams {
  @Parameter(names = Array("--temp-path"), description = "Path to temp dir for parquet ingest", required = true)
  var tempDir: String = _
}
