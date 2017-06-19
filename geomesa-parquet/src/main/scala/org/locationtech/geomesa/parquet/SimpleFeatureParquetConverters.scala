package org.locationtech.geomesa.parquet

import com.vividsolutions.jts.geom.Coordinate
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


class SimpleFeatureGroupConverter(sft: SimpleFeatureType) extends GroupConverter {

  private val idConverter = new PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      current.getIdentifier.asInstanceOf[FeatureIdImpl].setID(value.toStringUsingUTF8)
    }
  }
  private val converters = SimpleFeatureParquetConverters.converters(sft, this) :+ idConverter
  private val numAttributes = sft.getAttributeCount

  var current: SimpleFeature = _

  override def start(): Unit = {
    current = new ScalaSimpleFeature("", sft)
  }

  override def end(): Unit = { }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

}

abstract class SimpleFeatureFieldConverter(parent: SimpleFeatureGroupConverter) extends PrimitiveConverter

class PointConverter(parent: SimpleFeatureGroupConverter) extends GroupConverter {

  private val gf = JTSFactoryFinder.getGeometryFactory
  private var x: Double = _
  private var y: Double = _

  private val converters = Array[PrimitiveConverter](
    // Bind to this specific point converter
    new PrimitiveConverter {
      override def addDouble(value: Double): Unit = {
        x = value
      }
    },
    new PrimitiveConverter {
      override def addDouble(value: Double): Unit = {
        y = value
      }
    }
  )

  override def getConverter(fieldIndex: Int): Converter = {
    converters(fieldIndex)
  }

  override def start(): Unit = {}

  override def end(): Unit = {
    parent.current.setDefaultGeometry(gf.createPoint(new Coordinate(x, y)))
  }
}

object SimpleFeatureParquetConverters {

  def converters(sft: SimpleFeatureType, sfGC: SimpleFeatureGroupConverter): Array[Converter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, idx) => converterFor(ad, idx, sfGC) }.toArray
  }

  def converterFor(ad: AttributeDescriptor, index: Int, parent: SimpleFeatureGroupConverter): Converter = {
    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)

    objectType match {

      case ObjectType.GEOMETRY =>
        // TODO support union type of other geometries based on the SFT
        new PointConverter(parent)

      case ObjectType.DATE =>
        new SimpleFeatureFieldConverter(parent) {
          override def addLong(value: Long): Unit = {
            parent.current.setAttribute(index, value)
          }
        }

      case ObjectType.STRING =>
        new SimpleFeatureFieldConverter(parent) {
          override def addBinary(value: Binary): Unit = {
            parent.current.setAttribute(index, value.toStringUsingUTF8)
          }
        }

      case ObjectType.INT =>
        new SimpleFeatureFieldConverter(parent) {
          override def addInt(value: Int): Unit = {
            parent.current.setAttribute(index, value)
          }
        }

      case ObjectType.DOUBLE =>
        new SimpleFeatureFieldConverter(parent) {
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
        new SimpleFeatureFieldConverter(parent) {
          override def addLong(value: Long): Unit = {
            parent.current.setAttribute(index, value)
          }
        }


      case ObjectType.FLOAT =>
        new SimpleFeatureFieldConverter(parent) {
          override def addFloat(value: Float): Unit = {
            parent.current.setAttribute(index, value)
          }
        }

      case ObjectType.BOOLEAN =>
        new SimpleFeatureFieldConverter(parent) {
          override def addBoolean(value: Boolean): Unit = {
            parent.current.setAttribute(index, value)
          }
        }


      case ObjectType.BYTES =>
        new SimpleFeatureFieldConverter(parent) {
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
        // TODO: binary storage :)
        null
    }

  }

}
