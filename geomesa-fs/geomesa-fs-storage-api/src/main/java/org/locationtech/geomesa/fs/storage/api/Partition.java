package org.locationtech.geomesa.fs.storage.api;

import java.io.IOException;
import java.net.URI;

/**
 * Abstarct class defining a partition. Two partitions are equal if they have the same name.
 */
abstract public class Partition {
    protected final String name;

    public Partition(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    // TODO consider using Path instead of URI? but taht binds us to hadoop
    abstract public java.util.List<URI> getPaths() throws IOException;

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

