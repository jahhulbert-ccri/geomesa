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

import org.opengis.feature.simple.SimpleFeatureType

/**
  * Created by ahulbert on 6/27/17.
  */
object Stupid {

  def makeScheme(sft: SimpleFeatureType) = new DateTimeZ2Scheme(DateTimeFormatter.ofPattern("yyyy/DDD/HH"), ChronoUnit.HOURS, 1, 2, sft, "dtg", "geom")
}
