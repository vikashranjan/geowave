#!/bin/bash
export STAGING_DIR=/tmp
export INSTANCE=accumulo
export TIME_REGEX=2015111[34]
export EAST=49.04694
export WEST=48.658291
export NORTH=2.63791
export SOUTH=2.08679
export HDFS_PORT=8020
export RESOURCE_MAN_PORT=8032
export NUM_PARTITIONS=32
export GEOSERVER_HOME=/usr/local/geowave/geoserver                             
export GEOSERVER_DATA_DIR=$GEOSERVER_HOME/data_dir
export GEOWAVE_TOOL_JAVA_OPT=-Xmx4g
export GEOWAVE_TOOLS_HOME=/usr/local/geowave/tools
export GEOWAVE_VERSION=0.9.1
