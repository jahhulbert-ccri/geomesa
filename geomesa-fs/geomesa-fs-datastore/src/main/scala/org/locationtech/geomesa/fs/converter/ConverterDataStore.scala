/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.converter

import org.apache.hadoop.fs.{Path, FileSystem}
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage



class ConverterDataStore(fs: FileSystem,
                         root: Path,
                         fileSystemStorage: FileSystemStorage,
                         namespaceStr: String = null)
  extends FileSystemDataStore(fs, root, fileSystemStorage, namespaceStr) {



}
