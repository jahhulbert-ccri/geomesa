#!/usr/bin/env bash

# Get the GEOMESA home
if [[ -z "${GEOMESA_HBASE_HOME}" ]]; then
  export GEOMESA_HBASE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# HDFS dynamic jars lib dir
HDFS_LIB_DIR=/hbase/lib

hadoop fs -test -d ${HDFS_LIB_DIR} || (hadoop fs -mkdir ${HDFS_LIB_DIR} && echo "Created lib dir ${HDFS_LIB_DIR}")
hadoop fs -test -d ${HDFS_LIB_DIR} || (echo "Error: lib dir cannot be accessed" && exit 1)

echo "Using HDFS lib dir ${HDFS_LIB_DIR}"


GEOMESA_JAR=$(find ${GEOMESA_HBASE_HOME}/dist/hbase -name "geomesa-hbase-distributed-runtime*")
if [[ "x$GEOMESA_JAR" == "x" ]]; then
    echo "Could not find GeoMesa distributed runtime JAR"
    exit 1
else
    echo "Using GeoMesa JAR: $GEOMESA_JAR"
fi

hadoop fs -put ${GEOMESA_JAR}  ${HDFS_LIB_DIR}/
REMOTE_JAR="${HDFS_LIB_DIR}/$(basename ${GEOMESA_JAR})"

(hadoop fs -test -f "${REMOTE_JAR}" && echo "Successfully installed coprocessors at ${REMOTE_JAR}") || (echo "Error installing coprocessors" && exit 1)


