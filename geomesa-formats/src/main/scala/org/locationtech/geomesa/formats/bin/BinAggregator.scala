package org.locationtech.geomesa.formats.bin

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, LineString, Point}
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.formats.bin.BinOptions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType


trait BinAggregator {

  var trackIndex: Int = -1
  var geomIndex: Int = -1
  var dtgIndex: Int = -1
  var labelIndex: Int = -1

  var isDtgArray: Boolean = false
  var getDtg: (KryoBufferSimpleFeature) => Long = null
  var linePointIndex: Int = -1

  var binSize: Int = 16
  var sort: Boolean = false

  var writeBin: (KryoBufferSimpleFeature, ByteBufferResult) => Unit = null

  var sft: SimpleFeatureType

  def init(options: Map[String, String]): ByteBufferResult = {
    geomIndex  = options(GEOM_OPT).toInt
    trackIndex = options(TRACK_OPT).toInt
    labelIndex = options.get(LABEL_OPT).map(_.toInt).getOrElse(-1)

    dtgIndex   = options(DATE_OPT).toInt
    isDtgArray = options.get(DATE_ARRAY_OPT).exists(_.toBoolean)
    getDtg     = createDtgFunction

    binSize = getBinSize
    sort = options(SORT_OPT).toBoolean

    writeBin = createWriteBin(sft)

    val batchSize = options(BATCH_SIZE_OPT).toInt * binSize

    val buffer = ByteBuffer.wrap(Array.ofDim(batchSize)).order(ByteOrder.LITTLE_ENDIAN)
    val overflow = ByteBuffer.wrap(Array.ofDim(binSize * 16)).order(ByteOrder.LITTLE_ENDIAN)

    new ByteBufferResult(buffer, overflow)
  }

  def createWriteBin(sft: SimpleFeatureType): (KryoBufferSimpleFeature, ByteBufferResult) => Unit = {
    // derive the bin values from the features
    val writeGeom: (KryoBufferSimpleFeature, ByteBufferResult) => Unit = if (sft.isPoints) {
      if (labelIndex == -1) writePoint else writePointWithLabel
    } else if (sft.isLines) {
      if (labelIndex == -1) writeLineString else writeLineStringWithLabel
    } else {
      if (labelIndex == -1) writeGeometry else writeGeometryWithLabel
    }
    writeGeom
  }

  def createDtgFunction: (KryoBufferSimpleFeature) => Long = {
    if (dtgIndex == -1) {
      (_) => 0L
    } else if (isDtgArray) {
      (sf) => {
        try {
          sf.getAttribute(dtgIndex).asInstanceOf[java.util.List[Date]].get(linePointIndex).getTime
        } catch {
          case e: IndexOutOfBoundsException => 0L
        }
      }
    } else {
      (sf) => sf.getDateAsLong(dtgIndex)
    }
  }

  def getBinSize: Int = if (labelIndex == -1) 16 else 24

  def encodeBinResult(result: ByteBufferResult): Array[Byte] = {
    val bytes = if (result.overflow.position() > 0) {
      // overflow bytes - copy the two buffers into one
      val copy = Array.ofDim[Byte](result.buffer.position + result.overflow.position)
      System.arraycopy(result.buffer.array, 0, copy, 0, result.buffer.position)
      System.arraycopy(result.overflow.array, 0, copy, result.buffer.position, result.overflow.position)
      copy
    } else if (result.buffer.position == result.buffer.limit) {
      // use the existing buffer if possible
      result.buffer.array
    } else {
      // if not, we have to copy it - values do not allow you to specify a valid range
      val copy = Array.ofDim[Byte](result.buffer.position)
      System.arraycopy(result.buffer.array, 0, copy, 0, result.buffer.position)
      copy
    }
    if (sort) {
      BinSorter.quickSort(bytes, 0, bytes.length - binSize, binSize)
    }
    bytes
  }

  /**
    * Writes a point to our buffer in the bin format
    */
  protected def writeBinToBuffer(sf: KryoBufferSimpleFeature, pt: Point, result: ByteBufferResult): Unit = {
    val buffer = result.ensureCapacity(16)
    val track = sf.getAttribute(trackIndex)
    if (track == null) {
      buffer.putInt(0)
    } else {
      buffer.putInt(track.hashCode())
    }
    buffer.putInt((getDtg(sf) / 1000).toInt)
    buffer.putFloat(pt.getY.toFloat) // y is lat
    buffer.putFloat(pt.getX.toFloat) // x is lon
  }

  /**
    * Writes a label to the buffer in the bin format
    */
  protected def writeLabelToBuffer(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    val label = sf.getAttribute(labelIndex)
    val labelAsLong = if (label == null) { 0L } else { Convert2ViewerFunction.convertToLabel(label.toString) }
    val buffer = result.ensureCapacity(8)
    buffer.putLong(labelAsLong)
  }

  /**
    * Writes a bin record from a feature that has a point geometry
    */
  def writePoint(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit =
    writeBinToBuffer(sf, sf.getAttribute(geomIndex).asInstanceOf[Point], result)

  /**
    * Writes point + label
    */
  def writePointWithLabel(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    writePoint(sf, result)
    writeLabelToBuffer(sf, result)
  }

  /**
    * Writes bins record from a feature that has a line string geometry.
    * The feature will be multiple bin records.
    */
  def writeLineString(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[LineString]
    linePointIndex = 0
    while (linePointIndex < geom.getNumPoints) {
      writeBinToBuffer(sf, geom.getPointN(linePointIndex), result)
      linePointIndex += 1
    }
  }

  /**
    * Writes line string + label
    */
  def writeLineStringWithLabel(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[LineString]
    linePointIndex = 0
    while (linePointIndex < geom.getNumPoints) {
      writeBinToBuffer(sf, geom.getPointN(linePointIndex), result)
      writeLabelToBuffer(sf, result)
      linePointIndex += 1
    }
  }

  /**
    * Writes a bin record from a feature that has a arbitrary geometry.
    * A single internal point will be written.
    */
  def writeGeometry(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    writeBinToBuffer(sf, sf.getAttribute(geomIndex).asInstanceOf[Geometry].safeCentroid(), result)
  }

  /**
    * Writes geom + label
    */
  def writeGeometryWithLabel(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    writeGeometry(sf, result)
    writeLabelToBuffer(sf, result)
  }
}

// wrapper for java's byte buffer that adds scala methods for the aggregating iterator
class ByteBufferResult(val buffer: ByteBuffer, var overflow: ByteBuffer) {
  def ensureCapacity(size: Int): ByteBuffer = {
    if (buffer.position < buffer.limit - size) {
      buffer
    } else if (overflow.position < overflow.limit - size) {
      overflow
    } else {
      val expanded = Array.ofDim[Byte](overflow.limit * 2)
      System.arraycopy(overflow.array, 0, expanded, 0, overflow.limit)
      val order = overflow.order
      val position = overflow.position
      overflow = ByteBuffer.wrap(expanded).order(order).position(position).asInstanceOf[ByteBuffer]
      overflow
    }
  }

  def isEmpty: Boolean = buffer.position == 0
  def clear(): Unit = {
    buffer.clear()
    overflow.clear()
  }
}

object BinOptions {
  final val RAW_SFT_OPT = "sft"

  final val BATCH_SIZE_OPT: String = "batch"
  final val SORT_OPT      : String = "sort"

  final val TRACK_OPT     : String = "track"
  final val GEOM_OPT      : String = "geom"
  final val DATE_OPT      : String = "dtg"
  final val LABEL_OPT     : String = "label"

  final val DATE_ARRAY_OPT: String = "dtg-array"
}

object BinAggregator {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints


  // need to be lazy to avoid class loading issues before init is called
  lazy val BinSft: SimpleFeatureType = SimpleFeatureTypes.createType("bin", "bin:Bytes,*geom:Point:srid=4326")
  val BinAttributeIndex: Int = 0 // index of 'bin' attribute in BIN_SFT
  val ZeroPoint: Point = WKTUtils.read("POINT(0 0)").asInstanceOf[Point]

  /**
    * Determines if the requested fields match the precomputed bin data
    */
  def canUsePrecomputedBins(sft: SimpleFeatureType, hints: Hints): Boolean = {
    sft.getBinTrackId.exists(_ == hints.getBinTrackIdField) &&
      hints.getBinGeomField.forall(_ == sft.getGeomField) &&
      hints.getBinDtgField == sft.getDtgField &&
      hints.getBinLabelField.isEmpty &&
      hints.getSampleByField.forall(_ == hints.getBinTrackIdField)
  }

  def getTrack(sf: SimpleFeature, i: Int): Int = {
    val t = sf.getAttribute(i)
    if (t == null) { 0 } else { t.hashCode }
  }

  // get a single geom
  def getPointGeom(sf: SimpleFeature, i: Int): (Float, Float) = {
    val p = sf.getAttribute(i).asInstanceOf[Point]
    (p.getY.toFloat, p.getX.toFloat)
  }

  // get a line geometry as an array of points
  def getLineGeom(sf: SimpleFeature, i: Int): Array[(Float, Float)] = {
    val geom = sf.getAttribute(i).asInstanceOf[LineString]
    (0 until geom.getNumPoints).map(geom.getPointN).map(p => (p.getY.toFloat, p.getX.toFloat)).toArray
  }

  // get a single geom
  def getGenericGeom(sf: SimpleFeature, i: Int): (Float, Float) = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    val p = sf.getAttribute(i).asInstanceOf[Geometry].safeCentroid()
    (p.getY.toFloat, p.getX.toFloat)
  }

  // get a single date
  def getDtg(sf: SimpleFeature, i: Int): Long = {
    val date = sf.getAttribute(i).asInstanceOf[Date]
    if (date == null) System.currentTimeMillis else date.getTime
  }

  // for line strings, we need an array of dates corresponding to the points in the line
  def getLineDtg(sf: SimpleFeature, i: Int): Array[Long] = {
    import scala.collection.JavaConversions._
    val dates = sf.getAttribute(i).asInstanceOf[java.util.List[Date]]
    if (dates == null) Array.empty else dates.map(_.getTime).toArray
  }

  // get a label as a long
  def getLabel(sf: SimpleFeature, i: Int): Long = {
    val lbl = sf.getAttribute(i)
    if (lbl == null) 0L else Convert2ViewerFunction.convertToLabel(lbl.toString)
  }
}