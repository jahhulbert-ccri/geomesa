/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;

public interface FileSystemStorageFactory {
    boolean canProcess(Map<String, String> params);
    FileSystemStorage create(URI path,
                             SimpleFeatureType sft,
                             PartitionScheme scheme,
                             Map<String, String> params);
    boolean canLoad(URI path);
    FileSystemStorage load(URI path, Map<String, String> params);
}
