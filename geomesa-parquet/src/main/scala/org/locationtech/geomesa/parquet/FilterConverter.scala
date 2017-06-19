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

      case eq: org.opengis.filter.PropertyIsEqualTo =>
        val name = eq.getExpression1.asInstanceOf[PropertyName].getPropertyName
        val value = eq.getExpression2.toString
        val jt = jType(name)

        FilterApi.eq[Binary, BinaryColumn](FilterApi.binaryColumn(name), Binary.fromString(value))
    }


  }

//  def column(name: String) = {
//    val ad = sft.getDescriptor(name)
//    val binding = ad.getType.getBinding
//    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)
//
//    objectType match {
//      case ObjectType.STRING =>
//        FilterApi.binaryColumn(name)
//    }
//  }

  def jType(name: String) = {
    val ad = sft.getDescriptor(name)
    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)

    objectType match {

      case ObjectType.STRING => classOf[String]

      case ObjectType.INT => classOf[java.lang.Integer]
    }
  }
}
