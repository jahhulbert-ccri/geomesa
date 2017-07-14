/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;

public interface StorageFormatFactory {
    // Create new stores
    boolean canProcess(Map<String, Serializable> params);
    StorageFormat create(FileSystem fs,
                         Path path,
                         Metadata metadata,
                         Configuration conf,
                         Map<String, Serializable> params);

    // Loading existing stores
    boolean canLoad(Metadata metadata);
    StorageFormat load(FileSystem fs,
                       Path path,
                       Metadata metadata,
                       Configuration conf);
}
