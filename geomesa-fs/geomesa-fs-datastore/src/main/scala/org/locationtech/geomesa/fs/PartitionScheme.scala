/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset}
import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.filter.FilterHelper.extractGeometries
import org.locationtech.geomesa.filter.{FilterHelper, FilterValues}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

case class Partition(name: String)

trait PartitionScheme {
  /**
    * Return the partition in which a SimpleFeature should be stored
    * @param sf
    * @return
    */
  def getPartition(sf: SimpleFeature): Partition

  /**
    * Return a list of partitions that the system needs to query
    * in order to satisfy a filter predicate
    * @param f
    * @return
    */
  def coveringPartitions(f: Filter): Seq[String]
}

object PartitionOpts {
  val DateTimeFormatOpt = "datetime-format"
  val PartitionAttribute = "partition-attribute"

  def parseDateTimeFormat(opts: Map[String, String]): DateTimeFormatter = {
    DateTimeFormatter.ofPattern(opts(DateTimeFormatOpt))
  }

  def partitionAttribute(opts: Map[String, String]): String = {
    opts(PartitionAttribute)
  }
}

// TODO remove step?
class DateScheme(fmt: DateTimeFormatter,
                 stepUnit: ChronoUnit,
                 step: Int,
                 sft: SimpleFeatureType,
                 partitionAttribute: String)
  extends PartitionScheme {
  private val index = sft.indexOf(partitionAttribute)
  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    Partition(fmt.format(instant))
  }

  override def coveringPartitions(f: Filter): Seq[String] = {
    // TODO: deal with more than just a single date range
    val interval = FilterHelper.extractIntervals(f, partitionAttribute).values
      .map { case (s,e) => (
        Instant.ofEpochMilli(s.getMillis).atZone(ZoneOffset.UTC),
        Instant.ofEpochMilli(e.getMillis).atZone(ZoneOffset.UTC)) }
    if (interval.isEmpty) {
      Seq.empty[String]
    } else {
      val (start, end) = interval.head
      val count = start.until(end, stepUnit)
      (0 until count.toInt).map { i => start.plus(step, stepUnit) }.map { i => fmt.format(i) }
    }
  }
}

class IntraHourScheme(minuteIntervals: Int, fmt: DateTimeFormatter, sft: SimpleFeatureType, partitionAttribute: String)
  extends PartitionScheme {
  private val index = sft.indexOf(partitionAttribute)
  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    val adjusted = instant.withMinute(minuteIntervals*(instant.getMinute/minuteIntervals))
    Partition(fmt.format(adjusted))
  }

  override def coveringPartitions(f: Filter): Seq[String] = {
    // TODO: deal with more than just a single date range
    val interval = FilterHelper.extractIntervals(f, partitionAttribute).values
      .map { case (s,e) => (
        Instant.ofEpochMilli(s.getMillis).atZone(ZoneOffset.UTC),
        Instant.ofEpochMilli(e.getMillis).atZone(ZoneOffset.UTC)) }
    if (interval.isEmpty) {
      Seq.empty[String]
    } else {
      val (start, end) = interval.head
      start.withMinute(minuteIntervals * (start.getMinute / minuteIntervals))
      end.withMinute(minuteIntervals * (end.getMinute / minuteIntervals))
      val count = start.until(end, ChronoUnit.MINUTES) / minuteIntervals
      (0 until count.toInt).map { i => start.plusMinutes(i * minuteIntervals) }.map { i => fmt.format(i) }
    }
  }
}

class Z2Scheme(bitWidth: Int, sft: SimpleFeatureType, geomAttribute: String) extends PartitionScheme {
  private val geomAttrIndex = sft.indexOf(geomAttribute)
  private val z2 = new ZCurve2D(bitWidth)
  private val digits = math.round(math.log10(math.pow(bitWidth, 2))).toInt

  override def getPartition(sf: SimpleFeature): Partition = {
    val pt = sf.getAttribute(geomAttrIndex).asInstanceOf[Point]
    val idx = z2.toIndex(pt.getX, pt.getY)
    Partition(idx.formatted(s"%0${digits}d"))
  }

  override def coveringPartitions(f: Filter): Seq[String] = {
    // TODO trim ranges based on files that actually exist...
    val geometries: FilterValues[Geometry] = {
      // TODO support something other than point geoms
      val extracted = extractGeometries(f, geomAttribute, true)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    if (geometries.disjoint) {
      return Seq.empty
    }

    val xy = geometries.values.map(GeometryUtils.bounds).head
    val ranges = z2.toRanges(xy._1, xy._2, xy._3, xy._4)
    val result = ranges
        .flatMap { ir =>
          ir.lower to ir.upper
        }.map(_.formatted(s"%0${digits}d"))

    result
  }
}

class DateTimeZ2Scheme(fmt: DateTimeFormatter,
                       stepUnit: ChronoUnit,
                       step: Int,
                       z2BitWidth: Int,
                       sft: SimpleFeatureType,
                       dateTimeAttribute: String,
                       geomAttribute: String) extends PartitionScheme {

  private val z2Scheme = new Z2Scheme(z2BitWidth, sft, geomAttribute)
  private val dateScheme = new DateScheme(fmt, stepUnit, step, sft, dateTimeAttribute)

  override def getPartition(sf: SimpleFeature): Partition = {
    Partition(dateScheme.getPartition(sf).name + "/" + z2Scheme.getPartition(sf).name)
  }

  override def coveringPartitions(f: Filter): Seq[String] = {
    val dateParts = dateScheme.coveringPartitions(f)
    val z2Parts = z2Scheme.coveringPartitions(f)
    for { d <- dateParts; z <- z2Parts } yield s"$d/$z"
  }
}

class HierarchicalPartitionScheme(partitionSchemes: Seq[PartitionScheme], sep: String) extends PartitionScheme {
  override def getPartition(sf: SimpleFeature): Partition =
    Partition(partitionSchemes.map(_.getPartition(sf).name).mkString(sep))

  override def coveringPartitions(f: Filter): Seq[String] =
    partitionSchemes.flatMap(_.coveringPartitions(f)).distinct

}

object PartitionScheme {

  def apply(): PartitionScheme = ???
}
