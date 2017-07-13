/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.awt.RenderingHints
import java.util.ServiceLoader
import java.util.concurrent.Callable
import java.{io, util}

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataAccessFactory, DataStore, DataStoreFactorySpi, Query}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, FileSystemStorageFactory}
import org.locationtech.geomesa.fs.storage.common.{FileMetadata, PartitionScheme}
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable

class FileSystemDataStore(fs: FileSystem,
                          val root: Path,
                          readThreads: Int,
                          conf: Configuration) extends ContentDataStore {
  import scala.collection.JavaConversions._
  import FileSystemDataStore._

  private val typeNames: mutable.ListBuffer[String] = {
    val b = mutable.ListBuffer.empty[String]
    if (fs.exists(root)) {
      fs.listStatus(root).filter(_.isDirectory).map(_.getPath.getName).foreach(b += _)
    }
    b
  }

  private def getStorageFactory(encoding: String): FileSystemStorageFactory = {
    import scala.collection.JavaConversions._

    val params = Map("fs.encoding" -> encoding)
    storageFactory.find(_.canProcess(params)).getOrElse {
      throw new IllegalArgumentException("Can't create storage factory with the provided params")
    }
  }

  private def typeStorage(typeName: String): FileSystemStorage =
    if (typeNames.contains(typeName)) {
      TypeStorageCache.get((root, typeName), new Callable[FileSystemStorage] {
        override def call(): FileSystemStorage = {
          val typePath = new Path(root, typeName).toUri
          val fac = storageFactory.find(_.canLoad(typePath)).getOrElse {
            throw new IllegalArgumentException("Can't create storage factory with the provided params")
          }
          fac.load(typePath, Map.empty[String, String])
        }
      })
    } else {
      throw new IllegalArgumentException(s"Unable to find type $typeName in root directory $root")
    }

  override def createTypeNames(): util.List[Name] = {
    if (fs.exists(root)) {
      fs.listStatus(root)
        .filter(_.isDirectory)
        .map(_.getPath.getName)
        .map(name => new NameImpl(getNamespaceURI, name)).toList
    } else {
      List.empty[Name]
    }
  }



  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    if (!typeNames.contains(entry.getTypeName)) {
      throw new IllegalArgumentException(s"Type name ${entry.getTypeNames} cannot be found")
    }
    val storage = typeStorage(entry.getTypeName)

    val sft =
      storage.listFeatureTypes().find { f => f.getTypeName.equals(entry.getTypeName) }
        .getOrElse(throw new RuntimeException(s"Could not find feature type ${entry.getTypeName}"))
    new FileSystemFeatureStore(entry, Query.ALL, fs, storage, readThreads)
  }


  override def createSchema(sft: SimpleFeatureType): Unit = {
    val typeName = sft.getTypeName
    if (typeNames.contains(typeName)) {
      throw new IllegalArgumentException(s"Type already exists: ${sft.getTypeName}")
    } else {
      TypeStorageCache.get((root, typeName), new Callable[FileSystemStorage] {
        override def call(): FileSystemStorage = {
          val fac = getStorageFactory(sft.getUserData.get("fs.encoding").toString)

          val uri = new Path(root, sft.getTypeName).toUri
          val scheme = PartitionScheme.extractFromSft(sft)

          val storage = fac.create(uri, sft, scheme, Map.empty[String, String])// TODO update param map with params e.g. compression for parquet
          typeNames += typeName
          storage
        }
      })
    }
  }

}

object FileSystemDataStore {
  import scala.collection.JavaConversions._
  val storageFactory = ServiceLoader.load(classOf[FileSystemStorageFactory]).iterator().toList

  val TypeStorageCache: Cache[(Path, String), FileSystemStorage] =
    CacheBuilder.newBuilder().build[(Path, String), FileSystemStorage]()


}


class FileSystemDataStoreFactory extends DataStoreFactorySpi {
  import FileSystemDataStoreParams._

  override def createDataStore(params: util.Map[String, io.Serializable]): DataStore = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    import scala.collection.JavaConversions._

    val path = new Path(PathParam.lookUp(params).asInstanceOf[String])

    val conf = new Configuration()
    val storage = storageFactory.iterator().filter(_.canProcess(params)).map(_.build(params)).headOption.getOrElse {
      throw new IllegalArgumentException("Can't create storage factory with the provided params")
    }
    val fs = path.getFileSystem(conf)

    val readThreads = Option(ReadThreadsParam.lookUp(params)).map(_.asInstanceOf[java.lang.Integer])
      .getOrElse(ReadThreadsParam.getDefaultValue.asInstanceOf[java.lang.Integer])

    val ds = new FileSystemDataStore(fs, path, readThreads, conf)
    Option(NamespaceParam.lookUp(params).asInstanceOf[String]).foreach(ds.setNamespaceURI)
    ds
  }

  override def createNewDataStore(params: util.Map[String, io.Serializable]): DataStore =
    createDataStore(params)

  override def isAvailable: Boolean = true

  override def canProcess(params: util.Map[String, io.Serializable]): Boolean =
    params.containsKey(PathParam.getName)

  override def getParametersInfo: Array[DataAccessFactory.Param] =
    Array(PathParam, NamespaceParam)

  override def getDisplayName: String = "File System (GeoMesa)"

  override def getDescription: String = "File System Based Data Store"

  override def getImplementationHints: util.Map[RenderingHints.Key, _] =
    new util.HashMap[RenderingHints.Key, Serializable]()
}

object FileSystemDataStoreParams {
  val PathParam            = new Param("fs.path", classOf[String], "Root of the filesystem hierarchy", true)

  val ConverterNameParam   = new Param("fs.options.converter.name", classOf[String], "Converter Name", false)
  val ConverterConfigParam = new Param("fs.options.converter.conf", classOf[String], "Converter Typesafe Config", false)
  val SftNameParam         = new Param("fs.options.sft.name", classOf[String], "SimpleFeatureType Name", false)
  val SftConfigParam       = new Param("fs.options.sft.conf", classOf[String], "SimpleFeatureType Typesafe Config", false)

  val ReadThreadsParam     = new Param("read-threads", classOf[java.lang.Integer], "Read Threads", false, 4)

  val NamespaceParam       = new Param("namespace", classOf[String], "Namespace", false)
}
