#!/usr/bin/env bash
# parameters
# $1  : IN_DIR
# $2  : OUT_DIR
# $3  : SERVICE_YAML
# $4  : FILE_LIST
# $5  : PLUGIN
# $6  : DESCRIPTION
# $7  : THREAD_NUM
# $8  : SESSION
# $9  : FARM_LOADING_ZONE


export MALLOC_ARENA_MAX=2
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_TOP_PAD_=131072
export MALLOC_MMAP_MAX_=65536

DPE_PORT=
IP=127.0.0.1
lang="_java"
FENAME=

######################################################################################################################
function in_files_exists {

while IFS='' read -r line || [[ -n "$line" ]]; do
 if [ ! -f "$IN_DIR/$line" ]
   then
     echo "Error: The file = $line does not exist";
     return 0
 fi
done < "$FILE_LIST"
}

######################################################################################################################
function define_dpe_port {
DPE_PORT=7002
dpe_port=0
while  [ $dpe_port == 0 ]
do
dpe_port=0
exec 6<>/dev/tcp/127.0.0.1/$DPE_PORT || dpe_port=1
if [ $dpe_port == 0 ]; then
let "DPE_PORT=DPE_PORT+10"
else break
fi
done
let "DPE_PORT=DPE_PORT-2"
echo "$DPE_PORT"
}

######################################################################################################################
function define_host_ip {
local _ip _line
    while IFS=$': \t' read -a _line ;do
        [ -z "${_line%inet}" ] &&
           _ip=${_line[${#_line[1]}>4?1:2]} &&
           [ "${_ip#127.0.0.1}" ] && IP=$_ip && return 0
      done< <(LANG=C /sbin/ifconfig)
}

######################################################################################################################

# -------------------- preparation ---------------------------------
HOST=$(hostname)
USER=$(id -un)

# CLARA_HOME

if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME is not defined. Exiting..."
    exit
fi

# define private clara port
define_dpe_port

# define this host IP
define_host_ip

# define local DPE name
FENAME=$IP%$DPE_PORT$lang


# JAVA_HOME
OS="`uname`"
case $OS in
  'Linux')

    MACHINE_TYPE=`uname -m`
    if [ ${MACHINE_TYPE} == 'x86_64' ]; then

  J_HOME=$CLARA_HOME/jre/linux-64/jre1.8.0_112
else
  J_HOME=$CLARA_HOME/jre/linux-i586/jre1.8.0_112
fi
    ;;
  'WindowsNT')
    OS='Windows'
    ;;
  'Darwin')

  J_HOME=$CLARA_HOME/jre/macosx-64/jre1.8.0_112.jre/Contents/Home
    ;;
  *) ;;
esac


# Input data directory
if [ "$1" == "undefined" ]; then
IN_DIR="$CLARA_HOME/data/in"
else
IN_DIR=$1
fi

# Output directory
if [ "$2" == "undefined" ]; then
OUT_DIR="$CLARA_HOME/data/out"
else
OUT_DIR="$2"
fi

# Composition yaml file
if [ "$3" == "undefined" ]; then
SERVICE_YAML="$PLUGIN/config/services.yaml"
else
SERVICE_YAML="$3"
fi

# List of files to be processed
if [ "$4" == "undefined" ]; then
FILE_LIST="$PLUGIN/config/files.list"
else
FILE_LIST="$4"
fi

# User service plugin
if [ "$5" == "undefined" ]; then
PLUGIN="$CLARA_HOME/plugins/clas12"
else
PLUGIN="$5"
fi

DESCRIPTION=$6

THREAD_NUM=$7

# Data processing session
if [ "$8" == "undefined" ]; then
SESSION="$USER"
else
SESSION="$8"
fi

FARM_LOADING_ZONE=$9

export JAVA_HOME="$J_HOME"
export PATH=$PATH:$JAVA_HOME/bin:$CLARA_HOME/bin
export CLAS12DIR="$PLUGIN"
export CLASSPATH="$CLARA_HOME/lib/*:$PLUGIN/lib/clas/*:$PLUGIN/lib/services/*"


#------------- running -------------------------------------

if [ in_files_exists ]
 then

LOG_FILE_DPE="$CLARA_HOME/log/$HOST-$USER-$DESCRIPTION-jfe.log"
LOG_FILE_ORC="$CLARA_HOME/log/$HOST-$USER-$DESCRIPTION-co.log"

echo "-------- Running Conditions ---------------"
echo " Start time         = "$(date)
echo " Clara distribution = $CLARA_HOME"
echo " Plugin directory   = $PLUGIN"
echo " Log file           = $LOG_FILE_DPE"
echo " Note               = Running as local Front-End"
echo " Threads request    = $THREAD_NUM"
echo "------------------------------------------"
echo

# start dpe
if ["$THREAD_NUM" == "72"]; then
export JAVA_OPTS="(-Xms40000m -Xmx40000m -XX:+UseNUMA -XX:+UseBiasedLocking)"
fi

$CLARA_HOME/bin/j_dpe --port $DPE_PORT --host $HOST --session $SESSION --max-sockets 5120 --report 5 --max-cores $THREAD_NUM 2>&1 | tee $LOG_FILE_DPE &
sleep 20

unset JAVA_OPTS

# Starting cloud orchestrator
if [ "$FARM_LOADING_ZONE" = "undefined" ]; then
  $CLARA_HOME/bin/j_cloud -f $FENAME -s $SESSION -F -i $IN_DIR -o $OUT_DIR -p $THREAD_NUM -t $THREAD_NUM $SERVICE_YAML $FILE_LIST 2>&1 | tee $LOG_FILE_ORC
else
   $CLARA_HOME/bin/j_cloud -f $FENAME -s $SESSION -F -B -L -i $IN_DIR -o $OUT_DIR -l $FARM_LOADING_ZONE -p $THREAD_NUM -t $THREAD_NUM $SERVICE_YAML $FILE_LIST 2>&1 | tee $LOG_FILE_ORC
fi

fi

$CLARA_HOME/bin/remove-dpe $DPE_PORT
