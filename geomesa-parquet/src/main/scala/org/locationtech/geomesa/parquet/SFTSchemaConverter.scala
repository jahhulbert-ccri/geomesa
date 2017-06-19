/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import com.vividsolutions.jts.geom.Coordinate
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, OriginalType, Type, Types}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Created by afox on 5/25/17.
  */
object SFTSchemaConverter {


  def apply(sft: SimpleFeatureType): MessageType = {
    import scala.collection.JavaConversions._
    val idField =
      Types.primitive(PrimitiveTypeName.BINARY, Repetition.REPEATED)
        .as(OriginalType.UTF8)
        .named("fid")

    // NOTE: idField goes at the end of the record
    new MessageType(sft.getTypeName, sft.getAttributeDescriptors.map(convertField) :+ idField)
  }

  def inverse(messageType: MessageType): SimpleFeatureType = {
    import scala.collection.JavaConversions._
    val sftBuilder = new SftBuilder
    val fields :+ idField = messageType.getFields.toList
    fields.foreach { c =>
      c.asPrimitiveType().getPrimitiveTypeName match {
        case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => sftBuilder.point(c.getName)
        case PrimitiveTypeName.INT64                => sftBuilder.date(c.getName) // TODO: need to use metadata to determine date
        case PrimitiveTypeName.BINARY               => sftBuilder.stringType(c.getName)
        case PrimitiveTypeName.INT32                => sftBuilder.intType(c.getName)
        case PrimitiveTypeName.INT64                => sftBuilder.longType(c.getName)
        case PrimitiveTypeName.DOUBLE               => sftBuilder.doubleType(c.getName)
        case _ => throw new RuntimeException("Implement me")
      }
    }
    sftBuilder.build(messageType.getName)
  }

  def convertField(ad: AttributeDescriptor): Type = {
    import PrimitiveTypeName._
    import Type.Repetition

    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)
    objectType match {
      case ObjectType.GEOMETRY =>
        // TODO: currently only dealing with Points packed into a 16 byte fixed array
        Types.buildGroup(Repetition.REQUIRED)
          .primitive(DOUBLE, Repetition.REQUIRED).named("x")
          .primitive(DOUBLE, Repetition.REQUIRED).named("y")
          .named(ad.getLocalName)

      case ObjectType.DATE =>
        Types.primitive(INT64, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.STRING =>
        Types.primitive(BINARY, Repetition.OPTIONAL)
          .as(OriginalType.UTF8)
          .named(ad.getLocalName)

      case ObjectType.INT =>
        Types.primitive(INT32, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.DOUBLE =>
        Types.primitive(DOUBLE, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.LONG =>
        Types.primitive(INT64, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.FLOAT =>
        Types.primitive(FLOAT, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.BOOLEAN =>
        Types.primitive(BOOLEAN, Repetition.OPTIONAL)
          .named(ad.getLocalName)

      case ObjectType.BYTES =>
        // TODO:
        null

      case ObjectType.LIST =>
        // TODO:
        null

      case ObjectType.MAP =>
        // TODO:
        null

      case ObjectType.UUID =>
        // TODO:
        null
    }
  }

  abstract class SimpleFeatureConverter(parent: SimpleFeatureGroupConverter) extends PrimitiveConverter {

  }



  class PointCoordConv(parent: PointConverter, xory: String) extends PrimitiveConverter {
    override def addDouble(value: Double): Unit = {
      xory match {
        case "x" => parent.x = value
        case "y" => parent.y = value
      }
    }
  }

  class PointConverter(parent: SimpleFeatureGroupConverter) extends GroupConverter {

    val gf = JTSFactoryFinder.getGeometryFactory

    private val converters = Array[PrimitiveConverter](
      new PointCoordConv(this, "x"),
      new PointCoordConv(this, "y")
    )

    var x: Double = _
    var y: Double = _

    override def getConverter(fieldIndex: Int) = {
      converters(fieldIndex)
    }

    override def start(): Unit = {
      val foo = this
    }

    override def end(): Unit = {
      val foo = this
      parent.current.setDefaultGeometry(gf.createPoint(new Coordinate(x, y)))
    }
  }

  def converters(sft: SimpleFeatureType, sfGC: SimpleFeatureGroupConverter): Array[Converter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, idx) => converterFor(ad, idx, sfGC) }.toArray
  }

  def converterFor(ad: AttributeDescriptor, index: Int, parent: SimpleFeatureGroupConverter ): Converter = {
    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)
    objectType match {

      case ObjectType.GEOMETRY =>
        new PointConverter(parent)

      case ObjectType.DATE =>
        new SimpleFeatureConverter(parent)  {
          override def addLong(value: Long): Unit = {
            parent.current.setAttribute(index, value)
          }
        }

      case ObjectType.STRING =>
        new SimpleFeatureConverter(parent)  {
          override def addBinary(value: Binary): Unit = {
            parent.current.setAttribute(index, value.toStringUsingUTF8)
          }
        }

      case ObjectType.INT =>
        new SimpleFeatureConverter(parent)  {
          override def addInt(value: Int): Unit = {
            parent.current.setAttribute(index, value)
          }
        }

      case ObjectType.DOUBLE =>
        new SimpleFeatureConverter(parent) {
          override def addInt(value: Int): Unit = {
            parent.current.setAttribute(index, value.toDouble)
          }

          override def addDouble(value: Double): Unit = {
            parent.current.setAttribute(index, value)
          }

          override def addFloat(value: Float): Unit = {
            parent.current.setAttribute(index, value.toDouble)
          }

          override def addLong(value: Long): Unit = {
            parent.current.setAttribute(index, value.toDouble)
          }
        }

      case ObjectType.LONG =>
        new SimpleFeatureConverter(parent) {
          override def addLong(value: Long): Unit = {
            parent.current.setAttribute(index, value)
          }
        }


      case ObjectType.FLOAT =>
        new SimpleFeatureConverter(parent) {
          override def addFloat(value: Float): Unit = {
            parent.current.setAttribute(index, value)
          }
        }

      case ObjectType.BOOLEAN =>
        new SimpleFeatureConverter(parent) {
          override def addBoolean(value: Boolean): Unit = {
            parent.current.setAttribute(index, value)
          }
        }


      case ObjectType.BYTES =>
        new SimpleFeatureConverter(parent) {
          override def addBinary(value: Binary): Unit = {
            parent.current.setAttribute(index, value.getBytes)
          }
        }

      case ObjectType.LIST =>
        // TODO:
        null

      case ObjectType.MAP =>
        // TODO:
        null

      case ObjectType.UUID =>
        // TODO:
        null
    }

  }

  def buildAttributeWriters(sft: SimpleFeatureType): Array[AttributeWriter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, i) => AttributeWriter(ad, i) }.toArray
  }

}
