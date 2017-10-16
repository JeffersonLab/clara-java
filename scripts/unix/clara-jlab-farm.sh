#!/usr/bin/env bash

# This is the script to start Clara for CLAS12 on JLAB farm.
# Users can use this to trigger reconstruction in their own PBS/Auger scripts.
# Assumes that $CLARA_HOME is set and points to the Clara installation, visible to JLAB farm resources.
# Author: gurjyan
# Date: 10.13.2017

# Parameters:
# $1 - input_dir
# $2 - output_dir
# $3 - number of threads
# $4 - session
# $5 - description
# $6 - files.list (data-set metadata)

if [ -z "${CLARA_HOME}" ]; then
    echo "Error: \$CLARA_HOME is not defined."
    exit 1
fi

export MALLOC_ARENA_MAX=2
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_TOP_PAD_=131072
export MALLOC_MMAP_MAX_=65536
export MALLOC_MMAP_MAX_=65536

export CLARA_MONITOR_FRONT_END="clara1601%9000_java"
export CLAS12DIR=$CLARA_HOME"/plugins/clas12"

${CLARA_HOME}/bin/kill-dpes

sleep $[ ( $RANDOM % 20 )  + 1 ]s

${CLARA_HOME}/lib/clara/run-clara -i $1 -o $2 -l /scratch/clara -t $3 -s $4 -d $5 -J "-XX:+UseNUMA -XX:+UseBiasedLocking" $CLARA_HOME/plugins/clas12/config/services.yaml $6
