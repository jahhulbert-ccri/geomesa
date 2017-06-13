/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{Filter, IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.iterators.KryoLazyAgeOffFilter.Options
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Abstract base implementation of an iterator that ages off data based on some strategy in a SimpleFeature
  * stored in GeoMesa. Clients can implement logic to determine whether or not a SimpleFeature should be kept
  * or not based on the accept method. SimpleFeatures not "accepted" will be aged off.
  *
  * Concrete implementations of this iterator can be configured on scan, minc, and majc to age off data
  *
  * Age off iterators can be stacked but this may have performance implications
  */
abstract class KryoLazyAgeOffFilter extends Filter {

  protected var spec: String = _
  protected var sft: SimpleFeatureType = _
  protected var kryo: KryoFeatureSerializer = _
  protected var reusableSF: KryoBufferSimpleFeature = _
  protected var reuseText: Text = _

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val copy = super[Filter].deepCopy(env).asInstanceOf[KryoLazyAgeOffFilter]
    copy.spec = spec
    copy.sft = sft

    val kryoOptions = if (copy.sft.getSchemaVersion < 9) SerializationOptions.none else SerializationOptions.withoutId
    copy.kryo = IteratorCache.serializer(copy.spec, kryoOptions)
    copy.reusableSF = kryo.getReusableFeature
    copy.reuseText = new Text()

    copy
  }

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {

    super[Filter].init(source, options, env)

    reuseText = new Text()
    spec = options.get(Options.Sft)
    sft = IteratorCache.sft(spec)

    val kryoOptions = if (sft.getSchemaVersion < 9) SerializationOptions.none else SerializationOptions.withoutId
    val kryo = IteratorCache.serializer(spec, kryoOptions)
    reusableSF = kryo.getReusableFeature
  }

  override def accept(k: Key, v: Value): Boolean = {
    reusableSF.setBuffer(v.get)
    accept(reusableSF)
  }

  /**
    * Should this SimpleFeature accepted/kept (true) or not (false). Features not accepted will be aged off
    * during major/minor compactions.
    *
    * @param sf
    * @return
    */
  def accept(sf: SimpleFeature): Boolean

}

object KryoLazyAgeOffFilter {
  object Options {
    val Sft = "sft"
  }
}