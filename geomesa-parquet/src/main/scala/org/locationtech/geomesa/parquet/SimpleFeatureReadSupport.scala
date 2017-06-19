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
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Created by anthony on 5/27/17.
  */
class SimpleFeatureReadSupport(sft: SimpleFeatureType) extends ReadSupport[SimpleFeature] {

  override def init(context: InitContext): ReadSupport.ReadContext = {
    new ReadSupport.ReadContext(SFTSchemaConverter(sft), Maps.newHashMap())
  }

  override def prepareForRead(configuration: Configuration,
                              keyValueMetaData: util.Map[String, String],
                              fileSchema: MessageType,
                              readContext: ReadSupport.ReadContext): RecordMaterializer[SimpleFeature] = {
    new SimpleFeatureRecordMaterializer(sft)
  }
}

class SimpleFeatureRecordMaterializer(sft: SimpleFeatureType) extends RecordMaterializer[SimpleFeature] {
  private var conv = new SimpleFeatureGroupConverter(sft)

  override def getRootConverter: GroupConverter = conv

  override def getCurrentRecord: SimpleFeature = conv.current
}

class SimpleFeatureGroupConverter(sft: SimpleFeatureType) extends GroupConverter {

  private val x = 1
  var current: SimpleFeature = _

  private val idConverter = new PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      current.getIdentifier.asInstanceOf[FeatureIdImpl].setID(value.toStringUsingUTF8)
    }
  }

  private val converters = SFTSchemaConverter.converters(sft, this) :+ idConverter
  private val numAttributes = sft.getAttributeCount

  override def start(): Unit = {
    current = new ScalaSimpleFeature("", sft)
    current.setAttributes(Array.ofDim[AnyRef](numAttributes))
  }

  override def end(): Unit = {
    // make a copy so we can reuse the next when converting the next record
    val ret = new ScalaSimpleFeature("", sft)
    ret.getIdentifier.asInstanceOf[FeatureIdImpl].setID(current.getID)
    ret.setAttributes(current.getAttributes)
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)


}

