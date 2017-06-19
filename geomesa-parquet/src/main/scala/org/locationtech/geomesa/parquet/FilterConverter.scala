package org.locationtech.geomesa.parquet

import org.apache.parquet.filter2.compat.FilterCompat.{Filter => ParFilter}
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.{Filter => GeoFilter}


class FilterConverter(sft: SimpleFeatureType) {

  def toParquet(filter: GeoFilter): FilterPredicate = {

    filter match {

      case and: org.opengis.filter.And =>
        FilterApi.and(toParquet(and.getChildren.get(0)), toParquet(and.getChildren.get(1)))

      case or: org.opengis.filter.Or =>
        FilterApi.and(toParquet(or.getChildren.get(0)), toParquet(or.getChildren.get(1)))

      case binop: org.opengis.filter.BinaryComparisonOperator =>
        val name = binop.getExpression1.asInstanceOf[PropertyName].getPropertyName
        val value = binop.getExpression2.toString
        binop match {
          case eq: org.opengis.filter.PropertyIsEqualTo =>
            FilterApi.eq(column(name), convert(name, value))
          case neq: org.opengis.filter.PropertyIsNotEqualTo =>
            FilterApi.notEq(column(name), convert(name, value))
          case lt: org.opengis.filter.PropertyIsLessThan =>
            FilterApi.lt(column(name), convert(name, value))
          case lte: org.opengis.filter.PropertyIsLessThanOrEqualTo =>
            FilterApi.ltEq(column(name), convert(name, value))
          case gt: org.opengis.filter.PropertyIsGreaterThan =>
            FilterApi.gt(column(name), convert(name, value))
          case gte: org.opengis.filter.PropertyIsGreaterThanOrEqualTo =>
            FilterApi.gtEq(column(name), convert(name, value))
        }

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
