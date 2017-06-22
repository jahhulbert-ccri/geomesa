package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * Created by ahulbert on 6/22/17.
 */
public interface PartitionScheme {

    /**
     * Return the partition in which a SimpleFeature should be stored
     * @param sf
     * @return
     */
    Partition getPartition(SimpleFeature sf);

    /**
     * Return a list of partitions that the system needs to query
     * in order to satisfy a filter predicate
     * @param f
     * @return
     */
    java.util.List<Partition> getConveringPartitions(Filter f);
}
