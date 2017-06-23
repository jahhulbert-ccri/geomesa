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
    List<SimpleFeatureType> listFeatureTypes();
    SimpleFeatureType getFeatureType(String typeName);

    void createNewFeatureType(SimpleFeatureType sft, PartitionScheme scheme);
    PartitionScheme getPartitionScheme(SimpleFeatureType sft);

    URI getFileSystemRoot(String typeName);

    Partition getPartition(String name);
    List<Partition> listPartitions(String typeName);

    // TODO need to figure out partition vs path for readers and writers
    // to handle having partition schemes that store a single file at a node
    // or multiple files.
    FileSystemPartitionIterator getPartitionReader(Query q, Partition partition);

    FileSystemWriter getWriter(String typeName, Partition partition);
}
