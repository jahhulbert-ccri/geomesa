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
