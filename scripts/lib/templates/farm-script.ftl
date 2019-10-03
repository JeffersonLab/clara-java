#!/bin/bash

<#-- export MALLOC_ARENA_MAX=2 -->
<#-- export MALLOC_MMAP_THRESHOLD_=131072 -->
<#-- export MALLOC_TRIM_THRESHOLD_=131072 -->
<#-- export MALLOC_TOP_PAD_=131072 -->
<#-- export MALLOC_MMAP_MAX_=65536 -->
<#-- export MALLOC_MMAP_MAX_=65536 -->

<#if farm.javaOpts??>
export JAVA_OPTS="${farm.javaOpts}"
</#if>

export CLARA_HOME="${clara.dir}"
export CLARA_MONITOR_FE="${clara.monitorFE!"129.57.70.24%9000_java"}"
export CLAS12DIR="${clas12.dir}"
export CLARA_USER_DATA="${user_data.dir}"

if ping -w 1 -c 1 129.57.32.100 &> /dev/null
then
export CCDB_CONNECTION=mysql://clas12reader@clasdb-farm.jlab.org/clas12
export RCDB_CONNECTION=mysql://rcdb@clasdb-farm.jlab.org/rcdb
fi

<#-- "$CLARA_HOME/bin/kill-dpes" -->

<#-- sleep $[ ( $RANDOM % 20 )  + 1 ]s -->

${farm.command}
