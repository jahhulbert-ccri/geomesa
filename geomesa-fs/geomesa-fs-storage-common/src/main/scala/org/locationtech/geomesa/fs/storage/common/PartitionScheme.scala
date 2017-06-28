/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.io.Serializable
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset}
import java.util
import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.data.DataAccessFactory.Param
import org.locationtech.geomesa.filter.FilterHelper.extractGeometries
import org.locationtech.geomesa.filter.{FilterHelper, FilterValues}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

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
  val PartitionOptsPrefix = "fs.partition-scheme.opts."
  val PartitionSchemeParam = new Param("fs.partition-scheme.name", classOf[String], "Partition scheme name", false)

  def apply(sft: SimpleFeatureType, dsParams: util.Map[String, Serializable]): PartitionScheme = {
    val pName = PartitionSchemeParam.lookUp(dsParams).toString
    import scala.collection.JavaConversions._
    val pOpts = dsParams.keySet.filter(_.startsWith(PartitionOptsPrefix)).map{opt =>
      opt.replace(PartitionOptsPrefix, "") -> dsParams.get(opt).toString
    }.toMap
    PartitionScheme(sft, pName, pOpts)
  }

  def apply(sft: SimpleFeatureType, pName: String, opts: Map[String, String]): PartitionScheme = {
    import PartitionOpts._
    pName match {
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
  override def getPartitionName(sf: SimpleFeature): String = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    fmt.format(instant)
  }

  override def getCoveringPartitions(f: Filter): java.util.List[String] = {
    // TODO: deal with more than just a single date range
    val interval = FilterHelper.extractIntervals(f, partitionAttribute).values
      .map { case (s,e) => (
        Instant.ofEpochMilli(s.getMillis).atZone(ZoneOffset.UTC),
        Instant.ofEpochMilli(e.getMillis).atZone(ZoneOffset.UTC)) }
    if (interval.isEmpty) {
      List.empty[String]
    } else {
      val (start, end) = interval.head
      val count = start.until(end, stepUnit)
      (0 until count.toInt).map { i => start.plus(step, stepUnit) }.map { i => fmt.format(i) }
    }
  }

  // ? is this a good idea
  override def maxDepth(): Int = fmt.toString.count(_ == '/')
}

class Z2Scheme(bitWidth: Int, sft: SimpleFeatureType, geomAttribute: String) extends PartitionScheme {
  private val geomAttrIndex = sft.indexOf(geomAttribute)
  private val z2 = new ZCurve2D(bitWidth)
  private val digits = math.round(math.log10(math.pow(bitWidth, 2))).toInt

  override def getPartitionName(sf: SimpleFeature): String = {
    val pt = sf.getAttribute(geomAttrIndex).asInstanceOf[Point]
    val idx = z2.toIndex(pt.getX, pt.getY)
    idx.formatted(s"%0${digits}d")
  }

  override def getCoveringPartitions(f: Filter): java.util.List[String] = {
    // TODO trim ranges based on files that actually exist...
    val geometries: FilterValues[Geometry] = {
      // TODO support something other than point geoms
      val extracted = extractGeometries(f, geomAttribute, true)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    if (geometries.disjoint) {
      return List.empty[String]
    }

    val xy = geometries.values.map(GeometryUtils.bounds).head
    val ranges = z2.toRanges(xy._1, xy._2, xy._3, xy._4)
    ranges
      .flatMap { ir =>
        ir.lower to ir.upper
      }.map(_.formatted(s"%0${digits}d"))
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

  override def getPartitionName(sf: SimpleFeature): String = {
    dateScheme.getPartitionName(sf) + "/" + z2Scheme.getPartitionName(sf)
  }

  override def getCoveringPartitions(f: Filter): java.util.List[String] = {
    import scala.collection.JavaConversions._
    val dateParts = dateScheme.getCoveringPartitions(f)
    val z2Parts = z2Scheme.getCoveringPartitions(f)
    for { d <- dateParts; z <- z2Parts } yield s"$d/$z"
  }

  override def maxDepth(): Int = z2Scheme.maxDepth() + dateScheme.maxDepth()
}

class HierarchicalPartitionScheme(partitionSchemes: Seq[PartitionScheme], sep: String) extends PartitionScheme {
  override def getPartitionName(sf: SimpleFeature): String =
    partitionSchemes.map(_.getPartitionName(sf)).mkString(sep)

  import scala.collection.JavaConversions._
  override def getCoveringPartitions(f: Filter): java.util.List[String] =
    partitionSchemes.flatMap(_.getCoveringPartitions(f)).distinct

  override def maxDepth(): Int =  partitionSchemes.map(_.maxDepth()).sum

}
