Installing GeoMesa HBase
========================

Installing from the Binary Distribution
---------------------------------------

GeoMesa HBase artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version (``$VERSION`` = |release|)
and untar it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution
    $ wget http://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-hbase-dist_2.11/$VERSION/geomesa-hbase-dist_2.11-$VERSION-bin.tar.gz
    $ tar xvf geomesa-hbase-dist_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-hbase-dist_2.11-$VERSION
    $ ls
    bin/  conf/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _hbase_install_source:

Building from Source
--------------------

GeoMesa HBase may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa HBase
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-hbase/geomesa-hbase-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_hbase_commandline:

Setting up the HBase Command Line Tools
---------------------------------------

GeoMesa HBase comes with a set of command line tools for managing HBase features located in
``geomesa-hbase_2.11-$VERSION/bin/`` of the binary distribution.

.. note::

    You can configure environment variables and classpath settings in ``geomesa-hbase_2.11-$VERSION/conf/geomesa-env.sh``.

In the ``geomesa-hbase_2.11-$VERSION`` directory, run ``bin/geomesa-hbase configure`` to set up the tools.

.. code-block:: bash

    $ bin/geomesa-hbase configure
    Using GEOMESA_HBASE_HOME = /path/to/geomesa-hbase_2.11-1.3.0
    Do you want to reset this? Y\n y
    Using GEOMESA_HBASE_HOME as set: /path/to/geomesa-hbase_2.11-1.3.0
    Is this intentional? Y\n y
    To persist the configuration please edit conf/geomesa-env.sh or update your bashrc file to include:
    export GEOMESA_HBASE_HOME=/path/to/geomesa-hbase_2.11-1.3.0
    export PATH=${GEOMESA_HBASE_HOME}/bin:$PATH

Update and re-source your ``~/.bashrc`` file to include the ``$GEOMESA_HBASE_HOME`` and ``$PATH`` updates.

.. note::

    ``geomesa-hbase`` will read the ``$HBASE_HOME`` and ``$HADOOP_HOME`` environment variables to load the
    appropriate JAR files for Hadoop and HBase. In addition, ``geomesa-hbase`` will prepend the value of the
    ``$GEOMESA_EXTRA_CLASSPATHS`` environment variable into the class path (giving it highest precedence). Use the
    ``geomesa classpath`` command in order to see what JARs are being used.

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

Run ``geomesa-hbase`` without arguments to confirm that the tools work.

.. code::

    $ bin/geomesa-hbase
    Using GEOMESA_HBASE_HOME = /path/to/geomesa-hbase_2.11-1.3.0
    INFO  Usage: geomesa-hbase [command] [command options]
      Commands:
      ...

.. _install_hbase_geoserver:

Installing GeoMesa HBase in GeoServer
-------------------------------------

The HBase GeoServer plugin is not bundled by default in a GeoMesa binary distribution
and should be built from source. Download the source distribution (see
:ref:`building_from_source`), go to the ``geomesa-hbase/geomesa-hbase-gs-plugin``
directory, and build the module using the ``hbase`` Maven profile:

.. code-block:: bash

    $ mvn clean install -Phbase

After building, extract ``target/geomesa-hbase-gs-plugin_2.11-$VERSION-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory. Note that this plugin contains a shaded JAR with HBase 1.1.5
bundled. If you require a different version, modify the ``pom.xml`` and rebuild following
the instructions above.

This distribution does not include the Hadoop or Zookeeper JARs; the following JARs
should be copied from the ``lib`` directory of your HBase or Hadoop installations into
GeoServer's ``WEB-INF/lib`` directory:

 * hadoop-annotations-2.5.1.jar
 * hadoop-auth-2.5.1.jar
 * hadoop-common-2.5.1.jar
 * hadoop-mapreduce-client-core-2.5.1.jar
 * hadoop-yarn-api-2.5.1.jar
 * hadoop-yarn-common-2.5.1.jar
 * zookeeper-3.4.6.jar
 * commons-configuration-1.6.jar

(Note the versions may vary depending on your installation.)

The HBase data store requires the configuration file ``hbase-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist).

Restart GeoServer after the JARs are installed.
