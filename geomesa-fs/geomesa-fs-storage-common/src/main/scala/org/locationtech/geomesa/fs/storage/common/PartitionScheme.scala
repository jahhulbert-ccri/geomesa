/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset}
import java.util
import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.filter.FilterHelper.extractGeometries
import org.locationtech.geomesa.filter.{FilterHelper, FilterValues}
import org.locationtech.geomesa.fs.storage.api.{Partition, PartitionScheme}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

object PartitionOpts {
  val DateTimeFormatOpt = "datetime-format"
  val StepUnitOpt = "step-unit"
  val StepOpt = "step"
  val PartitionAttribute = "partition-attribute"

  def parseDateTimeFormat(opts: Map[String, String]): DateTimeFormatter = {
    val fmtStr = opts(DateTimeFormatOpt)
    if (fmtStr.endsWith("/")) throw new IllegalArgumentException("Format cannot end with a slash")
    DateTimeFormatter.ofPattern(fmtStr)
  }

  def parseAttribute(opts: Map[String, String]): String = {
    opts(PartitionAttribute)
  }

  def parseStepUnit(opts: Map[String, String]): ChronoUnit = {
    ChronoUnit.valueOf(opts(StepUnitOpt))
  }

  def parseStep(opts: Map[String, String]): Int = {
    opts(StepOpt).toInt
  }
}


object PartitionScheme {

  def apply(name: String, sft: SimpleFeatureType, opts: Map[String, String]): PartitionScheme = {
    import PartitionOpts._
    name match {
      case "date" =>
        val attr = parseAttribute(opts)
        val fmt = parseDateTimeFormat(opts)
        val su = parseStepUnit(opts)
        val s = parseStep(opts)
        new DateScheme(fmt, su, s, sft, attr)
    }
  }
}

class DateScheme(fmt: DateTimeFormatter,
                 stepUnit: ChronoUnit,
                 step: Int,
                 sft: SimpleFeatureType,
                 partitionAttribute: String)
  extends PartitionScheme {
  private val index = sft.indexOf(partitionAttribute)
  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    new LeafStoragePartition(fmt.format(instant))
  }

  override def getCoveringPartitions(f: Filter): java.util.List[Partition] = {
    // TODO: deal with more than just a single date range
    val interval = FilterHelper.extractIntervals(f, partitionAttribute).values
      .map { case (s,e) => (
        Instant.ofEpochMilli(s.getMillis).atZone(ZoneOffset.UTC),
        Instant.ofEpochMilli(e.getMillis).atZone(ZoneOffset.UTC)) }
    val names = if (interval.isEmpty) {
      Seq.empty[String]
    } else {
      val (start, end) = interval.head
      val count = start.until(end, stepUnit)
      (0 until count.toInt).map { i => start.plus(step, stepUnit) }.map { i => fmt.format(i) }
    }
    names.map(new LeafStoragePartition(_).asInstanceOf[Partition]).toList.asJava
  }

  // ? is this a good idea
  override def maxDepth(): Int = fmt.toString.count(_ == '/')
}

//// TODO convert to using step unit and step and not be minute specific
//class IntraHourScheme(fmt: DateTimeFormatter,
//                      minuteIntervals: Int,
//                      sft: SimpleFeatureType,
//                      partitionAttribute: String)
//  extends PartitionScheme {
//  private val index = sft.indexOf(partitionAttribute)
//  override def getPartition(sf: SimpleFeature): Partition = {
//    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
//    val adjusted = instant.withMinute(minuteIntervals*(instant.getMinute/minuteIntervals))
//    new LeafStoragePartition(fmt.format(adjusted))
//  }
//
//  override def getCoveringPartitions(f: Filter): java.util.List[Partition] = {
//    // TODO: deal with more than just a single date range
//    val interval = FilterHelper.extractIntervals(f, partitionAttribute).values
//      .map { case (s,e) => (
//        Instant.ofEpochMilli(s.getMillis).atZone(ZoneOffset.UTC),
//        Instant.ofEpochMilli(e.getMillis).atZone(ZoneOffset.UTC)) }
//    val names = if (interval.isEmpty) {
//      Seq.empty[String]
//    } else {
//      val (start, end) = interval.head
//      start.withMinute(minuteIntervals * (start.getMinute / minuteIntervals))
//      end.withMinute(minuteIntervals * (end.getMinute / minuteIntervals))
//      val count = start.until(end, ChronoUnit.MINUTES) / minuteIntervals
//      (0 until count.toInt).map { i => start.plusMinutes(i * minuteIntervals) }.map { i => fmt.format(i) }
//    }
//    names.map(new LeafStoragePartition(_).asInstanceOf[Partition]).toList.asJava
//  }
//
//  override def maxDepth(): Int = fmt.toString.count(_ == '/')
//
//}

class Z2Scheme(bitWidth: Int, sft: SimpleFeatureType, geomAttribute: String) extends PartitionScheme {
  private val geomAttrIndex = sft.indexOf(geomAttribute)
  private val z2 = new ZCurve2D(bitWidth)
  private val digits = math.round(math.log10(math.pow(bitWidth, 2))).toInt

  override def getPartition(sf: SimpleFeature): Partition = {
    val pt = sf.getAttribute(geomAttrIndex).asInstanceOf[Point]
    val idx = z2.toIndex(pt.getX, pt.getY)
    new LeafStoragePartition(idx.formatted(s"%0${digits}d"))
  }

  override def getCoveringPartitions(f: Filter): java.util.List[Partition] = {
    // TODO trim ranges based on files that actually exist...
    val geometries: FilterValues[Geometry] = {
      // TODO support something other than point geoms
      val extracted = extractGeometries(f, geomAttribute, true)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    if (geometries.disjoint) {
      return new util.ArrayList[Partition]()
    }

    val xy = geometries.values.map(GeometryUtils.bounds).head
    val ranges = z2.toRanges(xy._1, xy._2, xy._3, xy._4)
    val result = ranges
        .flatMap { ir =>
          ir.lower to ir.upper
        }.map(_.formatted(s"%0${digits}d"))

    result.map(new LeafStoragePartition(_).asInstanceOf[Partition]).toList.asJava
  }

  override def maxDepth(): Int = 1
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
    val x = dateScheme.getPartition(sf).getName + "/" + z2Scheme.getPartition(sf).getName
    new LeafStoragePartition(x)
  }

  override def getCoveringPartitions(f: Filter): java.util.List[Partition] = {
    import scala.collection.JavaConversions._
    val dateParts = dateScheme.getCoveringPartitions(f)
    val z2Parts = z2Scheme.getCoveringPartitions(f)
    val partNames = for { d <- dateParts; z <- z2Parts } yield s"${d.getName}/${z.getName}"
    partNames.map(new LeafStoragePartition(_).asInstanceOf[Partition]).toList
  }

  override def maxDepth(): Int = z2Scheme.maxDepth() + dateScheme.maxDepth()
}

class HierarchicalPartitionScheme(partitionSchemes: Seq[PartitionScheme], sep: String) extends PartitionScheme {
  override def getPartition(sf: SimpleFeature): Partition =
    new LeafStoragePartition(partitionSchemes.map(_.getPartition(sf).getName).mkString(sep))

  import scala.collection.JavaConversions._
  override def getCoveringPartitions(f: Filter): java.util.List[Partition] =
    partitionSchemes.flatMap(_.getCoveringPartitions(f)).distinct

  override def maxDepth(): Int =  partitionSchemes.map(_.maxDepth()).sum

}
