/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.accumulo.AccumuloQueryPlannerType
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.spatial.BBOX
import org.opengis.geometry.BoundingBox

/**
 * Executes a query against geomesa
 */
class AccumuloQueryPlanner(ds: AccumuloDataStore) extends AccumuloQueryPlannerType(ds) {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  override protected [geomesa] def configureQuery(sft: SimpleFeatureType, original: Query): Query = {
    val query = super.configureQuery(sft, original)
    // add the bbox from the density query to the filter
    if (query.getHints.isDensityQuery) {
      val env = query.getHints.getDensityEnvelope.get.asInstanceOf[ReferencedEnvelope]
      val bbox = ff.bbox(ff.property(sft.getGeometryDescriptor.getLocalName), env)
      if (query.getFilter == Filter.INCLUDE) {
        query.setFilter(bbox)
      } else {
        // add the bbox - try to not duplicate an existing bbox
        def compareDbls(d1: Double, d2: Double): Boolean = math.abs(d1 - d2) < 0.0001 // our precision
        def compare(b1: BoundingBox, b2: BoundingBox): Boolean = {
          compareDbls(b1.getMinX, b2.getMinX) && compareDbls(b1.getMaxX, b2.getMaxX) &&
              compareDbls(b1.getMinY, b2.getMinY) && compareDbls(b1.getMaxY, b2.getMaxY)
        }
        val filters = decomposeAnd(query.getFilter).filter {
          case b: BBOX if compare(b.getBounds, bbox.getBounds) => false
          case _ => true
        }
        query.setFilter(andFilters(filters ++ Seq(bbox)))
      }
    }
    query
  }

  // This function calculates the SimpleFeatureType of the returned SFs.
  override protected [geomesa] def getReturnSft(sft: SimpleFeatureType, hints: Hints): SimpleFeatureType = {
    if (hints.isBinQuery) {
      BinaryOutputEncoder.BinEncodedSft
    } else if (hints.isArrowQuery) {
      org.locationtech.geomesa.arrow.ArrowEncodedSft
    } else if (hints.isDensityQuery) {
      DensityScan.DensitySft
    } else if (hints.isStatsQuery) {
      KryoLazyStatsUtils.StatsSft
    } else if (hints.isMapAggregatingQuery) {
      val spec = KryoLazyMapAggregatingIterator.createMapSft(sft, hints.getMapAggregatingAttribute)
      SimpleFeatureTypes.createType(sft.getTypeName, spec)
    } else {
      super.getReturnSft(sft, hints)
    }
  }
}
