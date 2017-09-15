#!/usr/bin/env bash
# author vhg
# date 1.13.17


if [ -z ${1+x} ]; then
echo "Usage:"
echo "clara-orchestrator <author> [<threads-number> <clara-home> <plugin-dir> <in-dir> <out-dir>]"
echo "<author>         Clara data processing author"
echo
echo "optional parameters:"
echo "<threads-number>  number of parallel threads (default = 72)"
echo "<clara-home       Clara home (default = $CLARA_HOME env variable)"
echo "<plugin-dir>      Clara plugin directory (default = $CLAS12DIR env variable)"
echo "<in-dir           Input data directory (default = $CLARA_HOME/../data/in"
echo "<out-dir>         Output directory (default = $CLARA_HOME/../data/out"
exit;
fi

SESSION=$1

if [ -z ${2+x} ]; then THREAD_NUM=72 ; else THREAD_NUM=$2; fi
if ! [ -z ${3+x} ]; then CLARA_HOME=$3; fi
if ! [ -z ${4+x} ]; then CLAS12DIR=$4; fi

export CLARA_HOME
export CLAS12DIR

if [ -z ${5+x} ]; then IN_DIR="$CLARA_HOME/../data/in"; else IN_DIR=$5; fi
if [ -z ${6+x} ]; then OUT_DIR="$CLARA_HOME/../data/out"; else OUT_DIR=$6; fi


export CCDB_DATABASE="etc/data/database/clas12database.sqlite"
export PATH=$PATH:$CLARA_HOME/bin


SERVICE_YAML="$CLAS12DIR/config/services.yaml"
FILE_LIST="$CLAS12DIR/config/files.list"

j_cloud -s $SESSION -F -i $IN_DIR -o $OUT_DIR -p $THREAD_NUM -t $THREAD_NUM $SERVICE_YAML $FILE_LIST

