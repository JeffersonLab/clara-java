#!/bin/bash

export MALLOC_ARENA_MAX=2
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_TOP_PAD_=131072
export MALLOC_MMAP_MAX_=65536
export MALLOC_MMAP_MAX_=65536

export CLARA_HOME="${clara.dir}"
export CLAS12DIR="${clas12.dir}"

"$CLARA_HOME/bin/remove-dpe"

sleep $[ ( $RANDOM % 20 )  + 1 ]s

${farm.command}
