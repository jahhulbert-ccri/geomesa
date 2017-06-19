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
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.MessageType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

// TODO this needs to handle the rest of the filter not handled by parquet?
class SimpleFeatureReadSupport(sft: SimpleFeatureType) extends ReadSupport[SimpleFeature] {

  override def init(context: InitContext): ReadSupport.ReadContext = {
    new ReadSupport.ReadContext(SimpleFeatureParquetSchema(sft), Maps.newHashMap())
  }

  override def prepareForRead(configuration: Configuration,
                              keyValueMetaData: util.Map[String, String],
                              fileSchema: MessageType,
                              readContext: ReadSupport.ReadContext): RecordMaterializer[SimpleFeature] = {
    new SimpleFeatureRecordMaterializer(sft)
  }
}

class SimpleFeatureRecordMaterializer(sft: SimpleFeatureType) extends RecordMaterializer[SimpleFeature] {
  private var converter = new SimpleFeatureGroupConverter(sft)

  override def getRootConverter: GroupConverter = converter

  override def getCurrentRecord: SimpleFeature = converter.current
}
