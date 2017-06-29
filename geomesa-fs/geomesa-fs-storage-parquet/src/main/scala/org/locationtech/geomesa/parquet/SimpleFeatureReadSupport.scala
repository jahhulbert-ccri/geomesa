/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.util

import com.google.common.collect.Maps
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.MessageType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

// TODO this needs to handle the rest of the filter not handled by parquet?
class SimpleFeatureReadSupport extends ReadSupport[SimpleFeature] {

  private var sft: SimpleFeatureType = _

  override def init(context: InitContext): ReadSupport.ReadContext = {
    this.sft = SimpleFeatureReadSupport.sftFromConf(context.getConfiguration)
    new ReadSupport.ReadContext(SimpleFeatureParquetSchema(sft), Maps.newHashMap())
  }

  override def prepareForRead(configuration: Configuration,
                              keyValueMetaData: util.Map[String, String],
                              fileSchema: MessageType,
                              readContext: ReadSupport.ReadContext): RecordMaterializer[SimpleFeature] = {
    new SimpleFeatureRecordMaterializer(sft)
  }
}

object SimpleFeatureReadSupport {
  // HACK we need this for now
  val SftNameKey = "geomesa.sft.name"
  val SftConfKey = "geomesa.sft.conf"


  def updateConf(sft: SimpleFeatureType, conf: Configuration) = {
    conf.set(SftNameKey, sft.getTypeName)
    conf.set(SftConfKey, SimpleFeatureTypes.toConfig(sft).root().render(ConfigRenderOptions.concise()))
  }

  def sftFromConf(conf: Configuration): SimpleFeatureType = {
    val name = conf.get(SftNameKey)
    val confStr = conf.get(SftConfKey)
    val config = ConfigFactory.parseString(confStr)
    SimpleFeatureTypes.createType(config, Some(name), Some(s"geomesa.sfts.$name"))
  }
}

class SimpleFeatureRecordMaterializer(sft: SimpleFeatureType) extends RecordMaterializer[SimpleFeature] {
  private val converter = new SimpleFeatureGroupConverter(sft)
  override def getRootConverter: GroupConverter = converter
  override def getCurrentRecord: SimpleFeature = converter.current
}
