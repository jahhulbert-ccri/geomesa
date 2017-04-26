/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.ByteArrayOutputStream
import java.util.Date
import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureDeserializers}
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction, ExtendedValues}
import org.locationtech.geomesa.formats.bin.BinOptions._
import org.locationtech.geomesa.formats.bin.{BinAggregator, ByteBufferResult}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
 * Iterator that computes and aggregates 'bin' entries
 */
class BinAggregatingIterator
    extends KryoLazyAggregatingIterator[ByteBufferResult] with SamplingIterator with LazyLogging with BinAggregator {

  var sampling: Option[(SimpleFeature) => Boolean] = null

  override def init(options: Map[String, String]): ByteBufferResult = {
    sampling = sample(options)
    super.init(options)
  }

  override def createWriteBin(sft: SimpleFeatureType): (KryoBufferSimpleFeature, ByteBufferResult) => Unit = {
    sampling match {
      case None => super.createWriteBin(sft)
      case Some(samp) => (sf, bb) => if (samp(sf)) { super.createWriteBin(sft)(sf, bb) }
    }
  }

  override def notFull(result: ByteBufferResult): Boolean = result.buffer.position < result.buffer.limit

  override def aggregateResult(sf: SimpleFeature, result: ByteBufferResult): Unit =
    writeBin(sf.asInstanceOf[KryoBufferSimpleFeature], result)

  override def encodeResult(result: ByteBufferResult): Array[Byte] = encodeBinResult(result)

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new NotImplementedError()

}

/**
 * Aggregates bins that have already been computed as accumulo data values
 */
class PrecomputedBinAggregatingIterator extends BinAggregatingIterator {

  var decodeBin: (Array[Byte]) => SimpleFeature = null
  var setDate: (SimpleFeature, Long) => Unit = null
  var writePrecomputedBin: (SimpleFeature, ByteBufferResult) => Unit = null

  override def init(options: Map[String, String]): ByteBufferResult = {
    import KryoLazyAggregatingIterator._

    val result = super.init(options)

    val filter = options.contains(CQL_OPT)
    val dedupe = options.contains(DUPE_OPT)
    val sample = options.contains(SamplingIterator.SAMPLE_BY_OPT)

    val sf = new ScalaSimpleFeature("", sft)
    val gf = new GeometryFactory

    val index = try { AccumuloFeatureIndex.index(options(INDEX_OPT)) } catch {
      case NonFatal(e) => throw new RuntimeException(s"Index option not configured correctly: ${options.get(INDEX_OPT)}")
    }
    val getId = index.getIdFromRow(sft)

    // we only need to decode the parts required for the filter/dedupe/sampling check
    // note: we wouldn't be using precomputed if sample by field wasn't the track id
    decodeBin = if (filter) {
      setDate = if (isDtgArray) {
        (sf, long) => {
          val list = new java.util.ArrayList[Date](1)
          list.add(new Date(long))
          sf.setAttribute(dtgIndex, list)
        }
      } else {
        (sf, long) => sf.setAttribute(dtgIndex, new Date(long))
      }
      (_) => {
        val row = source.getTopKey.getRow
        sf.setId(getId(row.getBytes, 0, row.getLength))
        setValuesFromBin(sf, gf)
        sf
      }
    } else if (sample && dedupe) {
      (_) => {
        val row = source.getTopKey.getRow
        sf.setId(getId(row.getBytes, 0, row.getLength))
        setTrackIdFromBin(sf)
        sf
      }
    } else if (sample) {
      (_) => {
        setTrackIdFromBin(sf)
        sf
      }
    } else if (dedupe) {
      (_) => {
        val row = source.getTopKey.getRow
        sf.setId(getId(row.getBytes, 0, row.getLength))
        sf
      }
    } else {
      (_) => null
    }

    // we are using the pre-computed bin values - we can copy the value directly into our buffer
    writePrecomputedBin = sampling match {
      case None => (_, result) => {
        val bytes = source.getTopValue.get
        result.ensureCapacity(bytes.length).put(bytes)
      }
      case Some(samp) => (sf, result) => if (samp(sf)) {
        val bytes = source.getTopValue.get
        result.ensureCapacity(bytes.length).put(bytes)
      }
    }

    result
  }

  override def decode(value: Array[Byte]): SimpleFeature = decodeBin(value)

  override def aggregateResult(sf: SimpleFeature, result: ByteBufferResult): Unit =
    writePrecomputedBin(sf, result)

  /**
   * Writes a bin record into a simple feature for filtering
   */
  private def setValuesFromBin(sf: SimpleFeature, gf: GeometryFactory): Unit = {
    val values = Convert2ViewerFunction.decode(source.getTopValue.get)
    sf.setAttribute(geomIndex, gf.createPoint(new Coordinate(values.lat, values.lon)))
    sf.setAttribute(trackIndex, values.trackId)
    setDate(sf, values.dtg)
  }

  /**
   * Sets only the track id - used for sampling
   */
  private def setTrackIdFromBin(sf: SimpleFeature): Unit =
    sf.setAttribute(trackIndex, Convert2ViewerFunction.decode(source.getTopValue.get).trackId)

}

object BinAggregatingIterator extends LazyLogging {

  import org.locationtech.geomesa.formats.bin.BinAggregator._
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  val DEFAULT_PRIORITY = 25

  /**
   * Creates an iterator config that expects entries to be precomputed bin values
   */
  def configurePrecomputed(sft: SimpleFeatureType,
                           index: AccumuloFeatureIndexType,
                           filter: Option[Filter],
                           hints: Hints,
                           deduplicate: Boolean,
                           priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    sft.getBinTrackId match {
      case Some(trackId) =>
        val geom = sft.getGeomField
        val dtg = sft.getDtgField
        val batch = hints.getBinBatchSize
        val sort = hints.isBinSorting
        val sampling = hints.getSampling
        val is = configure(classOf[PrecomputedBinAggregatingIterator], sft, index, filter, trackId,
          geom, dtg, None, batch, sort, deduplicate, sampling, priority)
        is
      case None => throw new RuntimeException(s"No default trackId field found in SFT $sft")
    }
  }

  /**
   * Configure based on query hints
   */
  def configureDynamic(sft: SimpleFeatureType,
                       index: AccumuloFeatureIndexType,
                       filter: Option[Filter],
                       hints: Hints,
                       deduplicate: Boolean,
                       priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val trackId = hints.getBinTrackIdField
    val geom = hints.getBinGeomField.getOrElse(sft.getGeomField)
    val dtg = hints.getBinDtgField.orElse(sft.getDtgField)
    val label = hints.getBinLabelField
    val batchSize = hints.getBinBatchSize
    val sort = hints.isBinSorting
    val sampling = hints.getSampling

    configure(classOf[BinAggregatingIterator], sft, index, filter, trackId, geom, dtg,
      label, batchSize, sort, deduplicate, sampling, priority)
  }

  /**
   * Creates an iterator config that will operate on regular kryo encoded entries
   */
  private def configure(clas: Class[_ <: BinAggregatingIterator],
                        sft: SimpleFeatureType,
                        index: AccumuloFeatureIndexType,
                        filter: Option[Filter],
                        trackId: String,
                        geom: String,
                        dtg: Option[String],
                        label: Option[String],
                        batchSize: Int,
                        sort: Boolean,
                        deduplicate: Boolean,
                        sampling: Option[(Float, Option[String])],
                        priority: Int): IteratorSetting = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val is = new IteratorSetting(priority, "bin-iter", clas)
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    is.addOption(BATCH_SIZE_OPT, batchSize.toString)
    is.addOption(TRACK_OPT, sft.indexOf(trackId).toString)
    is.addOption(GEOM_OPT, sft.indexOf(geom).toString)
    val dtgIndex = dtg.map(sft.indexOf).getOrElse(-1)
    is.addOption(DATE_OPT, dtgIndex.toString)
    if (sft.isLines && dtgIndex != -1 && sft.getDescriptor(dtgIndex).isList &&
        classOf[Date].isAssignableFrom(sft.getDescriptor(dtgIndex).getListType())) {
      is.addOption(DATE_ARRAY_OPT, "true")
    }
    label.foreach(l => is.addOption(LABEL_OPT, sft.indexOf(l).toString))
    is.addOption(SORT_OPT, sort.toString)
    sampling.foreach(SamplingIterator.configure(is, sft, _))
    is
  }

  /**
   * Adapts the iterator to create simple features.
   * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
   */
  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", BinSft)
    sf.setAttribute(1, ZeroPoint)
    (e: Entry[Key, Value]) => {
      sf.setAttribute(BinAttributeIndex, e.getValue.get())
      sf
    }
  }

  /**
   * Fallback for when we can't use the aggregating iterator (for example, if the features are avro encoded).
   * Instead, do bin conversion in client.
   *
   * Only encodes one bin (or one bin line) per feature
   */
  def nonAggregatedKvsToFeatures(sft: SimpleFeatureType,
                                 index: AccumuloFeatureIndex,
                                 hints: Hints,
                                 serializationType: SerializationType): (Entry[Key, Value]) => SimpleFeature = {

    // don't use return sft from query hints, as it will be bin_sft
    val returnSft = hints.getTransformSchema.getOrElse(sft)

    val trackIdIndex = returnSft.indexOf(hints.getBinTrackIdField)
    val geomIndex = hints.getBinGeomField.map(returnSft.indexOf).getOrElse(returnSft.getGeomIndex)
    val dtgIndex= hints.getBinDtgField.map(returnSft.indexOf).getOrElse(returnSft.getDtgIndex.get)
    val labelIndexOpt= hints.getBinLabelField.map(returnSft.indexOf)

    val isPoint = returnSft.isPoints
    val isLineString = !isPoint && returnSft.isLines

    val encode: (SimpleFeature) => Array[Byte] = labelIndexOpt match {
      case None if isPoint =>
        (sf) => {
          val trackId = getTrack(sf, trackIdIndex)
          val (lat, lon) = getPointGeom(sf, geomIndex)
          val dtg = getDtg(sf, dtgIndex)
          Convert2ViewerFunction.encodeToByteArray(BasicValues(lat, lon, dtg, trackId))
        }

      case None if isLineString =>
        val buf = new ByteArrayOutputStream()
        (sf) => {
          buf.reset()
          val trackId = getTrack(sf, trackIdIndex)
          val points = getLineGeom(sf, geomIndex)
          val dtgs = getLineDtg(sf, dtgIndex)
          if (points.length != dtgs.length) {
            logger.warn(s"Mismatched geometries and dates for simple feature ${sf.getID} - skipping")
          } else {
            var i = 0
            while (i < points.length) {
              val (lat, lon) = points(i)
              Convert2ViewerFunction.encode(BasicValues(lat, lon, dtgs(i), trackId), buf)
              i += 1
            }
          }
          buf.toByteArray
        }

      case None =>
        (sf) => {
          val trackId = getTrack(sf, trackIdIndex)
          val (lat, lon) = getGenericGeom(sf, geomIndex)
          val dtg = getDtg(sf, dtgIndex)
          Convert2ViewerFunction.encodeToByteArray(BasicValues(lat, lon, dtg, trackId))
        }

      case Some(lblIndex) if isPoint =>
        (sf) => {
          val trackId = getTrack(sf, trackIdIndex)
          val (lat, lon) = getPointGeom(sf, geomIndex)
          val dtg = getDtg(sf, dtgIndex)
          val label = getLabel(sf, lblIndex)
          Convert2ViewerFunction.encodeToByteArray(ExtendedValues(lat, lon, dtg, trackId, label))
        }

      case Some(lblIndex) if isLineString =>
        val buf = new ByteArrayOutputStream()
        (sf) => {
          buf.reset()
          val trackId = getTrack(sf, trackIdIndex)
          val points = getLineGeom(sf, geomIndex)
          val dtgs = getLineDtg(sf, dtgIndex)
          val label = getLabel(sf, lblIndex)
          if (points.length != dtgs.length) {
            logger.warn(s"Mismatched geometries and dates for simple feature ${sf.getID} - skipping")
          } else {
            var i = 0
            while (i < points.length) {
              val (lat, lon) = points(i)
              Convert2ViewerFunction.encode(ExtendedValues(lat, lon, dtgs(i), trackId, label), buf)
              i += 1
            }
          }
          buf.toByteArray
        }

      case Some(lblIndex) =>
        (sf) => {
          val trackId = getTrack(sf, trackIdIndex)
          val (lat, lon) = getGenericGeom(sf, geomIndex)
          val dtg = getDtg(sf, dtgIndex)
          val label = getLabel(sf, lblIndex)
          Convert2ViewerFunction.encodeToByteArray(ExtendedValues(lat, lon, dtg, trackId, label))
        }
    }

    if (index.serializedWithId) {
      val deserializer = SimpleFeatureDeserializers(returnSft, serializationType)
      (e: Entry[Key, Value]) => {
        val deserialized = deserializer.deserialize(e.getValue.get())
        // set the value directly in the array, as we don't support byte arrays as properties
        new ScalaSimpleFeature(deserialized.getID, BinSft, Array(encode(deserialized), ZeroPoint))
      }
    } else {
      val getId = index.getIdFromRow(sft)
      val deserializer = SimpleFeatureDeserializers(returnSft, serializationType, SerializationOptions.withoutId)
      (e: Entry[Key, Value]) => {
        val deserialized = deserializer.deserialize(e.getValue.get())
        val row = e.getKey.getRow
        deserialized.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.getBytes, 0, row.getLength))
        // set the value directly in the array, as we don't support byte arrays as properties
        new ScalaSimpleFeature(deserialized.getID, BinSft, Array(encode(deserialized), ZeroPoint))
      }
    }

  }

}
