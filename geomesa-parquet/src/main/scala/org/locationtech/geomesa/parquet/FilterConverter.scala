/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.apache.parquet.filter2.predicate.Operators.BinaryColumn
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.FilterHelper.extractGeometries
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.PropertyName

import scala.collection.JavaConversions._


class FilterConverter(sft: SimpleFeatureType) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
  protected val geomAttr: String = sft.getGeomField
  protected val dtgAttrOpt: Option[String] = sft.getDtgField

  def convert(f: org.opengis.filter.Filter): Option[FilterPredicate] = {
    val filters = List(geoFilter(f), dtgFilter(f), attrFilter(f)).flatten
    if (filters.nonEmpty) {
      Some(filters.reduceLeft(FilterApi.and))
    } else{
      None
    }
  }

  protected def dtgFilter(f: org.opengis.filter.Filter): Option[FilterPredicate] = {
    dtgAttrOpt.map { dtgAttr =>
      val filters = FilterHelper.extractIntervals(f, dtgAttr).values.map { case (start, end) =>
        FilterApi.and(
          FilterApi.gtEq(FilterApi.longColumn(dtgAttr), start.getMillis: java.lang.Long),
          FilterApi.ltEq(FilterApi.longColumn(dtgAttr), end.getMillis: java.lang.Long)
        )
      }

      if (filters.nonEmpty) {
        Some(filters.reduceLeft(FilterApi.and))
      } else {
        None
      }
    }
  }.getOrElse(None)

  protected def geoFilter(f: org.opengis.filter.Filter): Option[FilterPredicate] = {
    val extracted = extractGeometries(f, geomAttr)
    if (extracted.isEmpty || extracted.disjoint) {
      None
    } else {
      val xy = extracted.values.map(GeometryUtils.bounds).reduce { (a, b) =>
        (math.min(a._1, b._1),
          math.min(a._2, b._2),
          math.max(a._3, b._3),
          math.max(a._4, b._4))
      }
      Some(
        List[FilterPredicate](
          FilterApi.gtEq(FilterApi.doubleColumn("geom.x"), Double.box(xy._1)),
          FilterApi.gtEq(FilterApi.doubleColumn("geom.y"), Double.box(xy._2)),
          FilterApi.ltEq(FilterApi.doubleColumn("geom.x"), Double.box(xy._3)),
          FilterApi.ltEq(FilterApi.doubleColumn("geom.y"), Double.box(xy._4))
       ).reduce(FilterApi.and)
      )
    }
  }

  protected def attrFilter(gtFilter: org.opengis.filter.Filter): Option[FilterPredicate] = {
    gtFilter match {

      case and: org.opengis.filter.And =>
        Option(and.getChildren.flatMap(attrFilter).reduceLeft(FilterApi.and))

      case or: org.opengis.filter.Or =>
        Option(or.getChildren.flatMap(attrFilter).reduceLeft(FilterApi.or))

      case binop: org.opengis.filter.BinaryComparisonOperator =>
        val name = binop.getExpression1.asInstanceOf[PropertyName].getPropertyName
        val value = binop.getExpression2.toString
        binop match {
          case eq: org.opengis.filter.PropertyIsEqualTo =>
            Option(FilterApi.eq(column(name), convert(name, value)))
          case neq: org.opengis.filter.PropertyIsNotEqualTo =>
            Option(FilterApi.notEq(column(name), convert(name, value)))
          case lt: org.opengis.filter.PropertyIsLessThan =>
            Option(FilterApi.lt(column(name), convert(name, value)))
          case lte: org.opengis.filter.PropertyIsLessThanOrEqualTo =>
            Option(FilterApi.ltEq(column(name), convert(name, value)))
          case gt: org.opengis.filter.PropertyIsGreaterThan =>
            Option(FilterApi.gt(column(name), convert(name, value)))
          case gte: org.opengis.filter.PropertyIsGreaterThanOrEqualTo =>
            Option(FilterApi.gtEq(column(name), convert(name, value)))
          case _ =>
            None

        }

      case _ =>
        None
      // TODO geotools based UDFs?
    }
  }

  // Todo support other things than Binary
  def column(name: String): BinaryColumn = {
    val ad = sft.getDescriptor(name)
    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)

    objectType match {
      case ObjectType.STRING =>
        FilterApi.binaryColumn(name)
    }
  }

  // Todo support other things than Binary
  def convert(name: String, value: AnyRef): Binary = {
    val ad = sft.getDescriptor(name)
    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)

    objectType match {

      case ObjectType.STRING => Binary.fromString(value.toString)

    }
  }
}
