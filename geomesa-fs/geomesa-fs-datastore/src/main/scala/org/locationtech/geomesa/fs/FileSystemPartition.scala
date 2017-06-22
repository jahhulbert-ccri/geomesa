package org.locationtech.geomesa.fs

import java.net.URI
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.api.Partition

/**
  * Created by ahulbert on 6/22/17.
  */
class FileSystemPartition(root: Path) extends Partition {
  override def getPaths: util.List[URI] = {
    for (i <- root.getFileSystem(new Configuration).listFiles(root, true)) yield i
  }


}
