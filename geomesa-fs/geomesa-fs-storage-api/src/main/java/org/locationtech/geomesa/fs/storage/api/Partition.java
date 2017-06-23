/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import java.io.IOException;
import java.net.URI;

/**
 * Abstract class defining a partition. Two partitions are equal if they have the same name.
 */
abstract public class Partition {
    protected final String name;

    protected Partition(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    // TODO this will likely be a list of URIs in the future for non-leaf partition strategies
    abstract public URI getPath();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Partition partition = (Partition) o;

        return name != null ? name.equals(partition.name) : partition.name == null;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "name='" + name + '\'' +
                '}';
    }
}

