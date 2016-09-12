#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

type=$1

function setGeoHome() {
  SOURCE="${BASH_SOURCE[0]}"
  # resolve $SOURCE until the file is no longer a symlink
  while [[ -h "${SOURCE}" ]]; do
    bin="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"
    SOURCE="$(readlink "${SOURCE}")"
    # if $SOURCE was a relative symlink, we need to resolve it relative to the path where
    # the symlink file was located
    [[ "${SOURCE}" != /* ]] && SOURCE="${bin}/${SOURCE}"
  done
  bin="$( cd -P "$( dirname "${SOURCE}" )" && cd ../ && pwd )"
  export %%gmtools.dist.name%%_HOME="$bin"
  export PATH=${%%gmtools.dist.name%%_HOME}/bin:$PATH
  if [[ -z "${GEOMESA_LOG_DIR}" ]]; then
    export GEOMESA_LOG_DIR="${%%gmtools.dist.name%%_HOME}/logs"
  fi
  if [[ ! -d "${GEOMESA_LOG_DIR}" ]]; then
    mkdir "${GEOMESA_LOG_DIR}"
  fi
  GEOMESA_LOG=${GEOMESA_LOG_DIR}/geomesa.err
  touch GEOMESA_LOG
  echo "Warning: %%gmtools.dist.name%%_HOME is not set, using $%%gmtools.dist.name%%_HOME" >> ${GEOMESA_LOG}
}

if [[ -z "$%%gmtools.dist.name%%_HOME" ]]; then
  setGeoHome
fi

NL=$'\n'
case "$type" in
  gdelt)
    read -p "Enter a date in the form YYYYMMDD: " DATE

    wget "http://data.gdeltproject.org/events/${DATE}.export.CSV.zip" -P $%%gmtools.dist.name%%_HOME/data/gdelt
    ;;

  geolife)
    wget "http://ftp.research.microsoft.com/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/Geolife Trajectories 1.3.zip" -P $%%gmtools.dist.name%%_HOME/data/geolife
    ;;

  osm-gpx)
    echo "Available regions: africa, asia, austrailia-oceania, canada, central-america,europe, ex-ussr, south-america, usa"
    read -p "Enter a region to download tracks for: " CONTINENT

    wget "http://zverik.osm.rambler.ru/gps/files/extracts/$CONTINENT.tar.xz" -P $%%gmtools.dist.name%%_HOME/data/osm-gpx
    ;;

  tdrive)
    echo "Note: each zip file contains approximately one million points"
    read -p "Download how many zip files? (14 total) " NUM

    UA="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36"

    for i in `seq 1 $NUM`; do
      echo "Downloading zip $i of $NUM"
      wget "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/0$i.zip" -P $%%gmtools.dist.name%%_HOME/data/tdrive -U "$UA" \
      || errorList="${errorList} https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/0${i}.zip ${NL}";
    done

    wget "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/User_guide_T-drive.pdf" -P $%%gmtools.dist.name%%_HOME/data/tdrive \
      || errorList="${errorList} https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/User_guide_T-drive.pdf";

    if [[ -n "${errorList}" ]]; then
      echo "Failed to download: ${NL} ${errorList}";
    fi

    ;;

  geonames)
    read -p "Enter the country code to download data for: " CC

    wget "http://download.geonames.org/export/dump/$CC.zip" -P $%%gmtools.dist.name%%_HOME/data/geonames
    ;;

  *)
    if [[ -n "$type" ]]; then
      PREFIX="Unknown data type '$type'."
    else
      PREFIX="Please enter a data type."
    fi
    echo "${PREFIX} Available types: gdelt, geolife, osm-gpx, tdrive, geonames"
    ;;

esac
