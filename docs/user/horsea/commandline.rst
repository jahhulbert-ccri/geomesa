GeoMesa FileSystem CommandLine Tools
====================================

Building from Source
--------------------

To build from source make sure you have Maven, Git, and Java SDK 1.8 installed. To build just the GeoMesa FileSystem
distribution do this:

.. code-block:: bash

    git clone git@github.com:locationtech/geomesa.git
    cd geomesa
    build/mvn clean install -pl geomesa-fs/geomesa-dist -am -T4

The tarball can also be downloaded from geomesa.org

Installation
------------

To install the command line tools simply untar the distribution tarball:

.. code-block:: bash

    # If building from source
    tar xvf geomesa-fs/geomesa-fs-dist/target/geomesa-fs_2.11-1.3.2-dist.tar.gz
    export GEOMESA_FS_HOME=/path/to/geomesa-fs_2.11-1.3.2

After untaring the archive you'll need to either define the standard Hadoop environmental variables or install hadoop
using the ``bin/install-hadoop.sh`` script provided in the tarball. Note that you will need the proper Yarn/Hadoop
environment configured if you would like to run a distributed ingest job to create files.

If you are using a service such as Amazon Elastic MapReduce (EMR) or have a distribution of Apache Hadoop, Cloudera, or
Hortonworks installed you can likely run something like this to configure hadoop for the tools:

.. code-block:: bash

    # These will be specific to your Hadoop environment
    . /etc/hadoop/conf/hadoop-env.sh
    . /etc/hadoop/conf/yarn-env.sh
    export HADOOP_CONF_DIR=/etc/hadoop/conf

After installing the tarball you should be able to run the ``geomesa-fs`` command like this:

.. code-block:: bash

    $ cd $GEOMESA_FS_HOME
    $ bin/geomesa-fs

The output should look like this:

.. code-block::

    INFO  Usage: geomesa-fs [command] [command options]
      Commands:
        classpath          Display the GeoMesa classpath
        configure          Configure the local environment for GeoMesa
        convert            Convert files using GeoMesa's internal converter framework
        describe-schema    Describe the attributes of a given GeoMesa feature type
        env                Examine the current GeoMesa environment
        export             Export features from a GeoMesa data store
        gen-avro-schema    Generate an Avro schema from a SimpleFeatureType
        get-sft-config     Get the SimpleFeatureType of a feature
        get-type-names     List GeoMesa feature type for a given Fs resource
        help               Show help
        ingest             Ingest/convert various file formats into GeoMesa
        update-metadata    Generate the metadata file
        version            Display the GeoMesa version installed locally

Command Overview
----------------

configure
~~~~~~~~~

Used to configure the current environment for using the commandline tools. This is frequently run after the tools are
first installed to ensure the environment is configured correctly::

    $ geomesa configure

classpath
~~~~~~~~~

Prints out the current classpath configuration::

    $ geomesa classpath

ingest
~~~~~~

Ingest files into a GeoMesa FS Datastore. Note that a "datastore" is simply a path in the filesystem. All data and
metadata will be stored in the filesystem under the hierarchy of the root path. Before ingesting data you will need
to define a SimpleFeatureType schema and a GeoMesa converter for your data. Many default type schemas and converters
for formats such as gdelt, twitter, etc. are provided in the conf directory of GeoMesa.

If you are not using one of these data types you can provide a file containing your schema and converter or configure
the GeoMesa tools to reference the schema configuration by name (link). Schemas files can also be stored in a remote
filesystem such as HDFS, S3, GCS, or WASB.

For example lets say we have all our data for 2016 stored in an S3 bucket::

    geomesa-fs ingest \
      -p s3a://mybucket/datastores/test \
      -e parquet \
      --partition-scheme daily,z2-2bits
      -s s3a://mybucket/schemas/my-config.conf \
      -C s3a://mybucket/schemas/my-config.conf \
      --num-reducers 20 \
      s3a://mybucket/data/2016/*


After ingest we expect to see a file structure with metadata and parquet files in S3 for our type named "myfeature"::

    aws s3 ls --recursive s3://mybucket/datastores/test

    datastores/test/myfeature/schema.sft
    datastores/test/myfeature/metadata
    datastores/test/myfeature/2016/01/01/0/0000.parquet
    datastores/test/myfeature/2016/01/01/2/0000.parquet
    datastores/test/myfeature/2016/01/01/3/0000.parquet
    datastores/test/myfeature/2016/01/02/0/0000.parquet
    datastores/test/myfeature/2016/01/02/1/0000.parquet
    datastores/test/myfeature/2016/01/02/3/0000.parquet

Two metadata files (``schema.sft`` and ``metadata``) store information about the schema, partition scheme, and list of
files that have been created. Note that the list of created files allows the datastore to quickly compute available
files to avoid possibly expensive directly listings against the filesystem. You may need to run update-metadata if you
decide to insert new files.

Notice that the bucket "directory structure" includes year, month, day and then a 0,1,2,3 representing a quadrant of the
Z2 Space Filling Curve with 2bit resolution (i.e. 0 = upper left, 1 = upper right, 2 = lower left, 3 = lower right).
Note that in our example Jan 1 and Jan 2 both do not have all four quadrants represented. This means that the input
dataset for that day didn't have any data in that region of the world. If additional data were ingested the directory
and a corresponding file would be created.


get-metadata
~~~~~~~~~~~~


update-metadata
~~~~~~~~~~~~~~~

Running

export
~~~~~~

