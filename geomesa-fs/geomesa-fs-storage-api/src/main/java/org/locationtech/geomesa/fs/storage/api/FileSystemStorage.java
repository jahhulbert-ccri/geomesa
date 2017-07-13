/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.geotools.data.Query;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

public interface FileSystemStorage {

    Partition getPartition(String name);
    List<Partition> listPartitions();

    FileSystemPartitionIterator getPartitionReader(Query q, Partition partition);

    FileSystemWriter getWriter(Partition partition);

    List<URI> getPaths(Partition partition);

    void updateMetadata();
    Metadata getMetadata();
}
