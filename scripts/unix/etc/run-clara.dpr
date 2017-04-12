#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 1.13.17


if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME is not defined. Exiting..."
    exit
fi

DPE_PORT=
FARM_DPE_PORT="undefined"
IP=127.0.0.1
lang="_java"
FENAME=

unset CCDB_DATABASE

######################################################################################################################
function print_logo {
printf "  ________   ____         ________   _________    ________\n"
printf " /        \ |    |       /        | |         \  /        | 4.3.0\n"
printf "|    |    | |    |      |   _._   | |    |    | |   _._   |\n"
printf "|    |____| |    |      |    |    | |        /  |    |    |\n"
printf "|    |    | |    |____  |    |    | |    .   -| |    |    |\n"
printf "|    |    | |         | |    |    | |    |    | |    |    |\n"
printf "|________/  |_________| |____|____| |____|____| |____|____|\n"
}

######################################################################################################################
function display_help {
echo
echo 'Usage: run-clara [option <operand>]'
echo
echo '  [-h | --help]'
echo '        Usage instructions'
echo
echo '  [-d | --description <description>]'
echo '        A single string (no spaces) describing data processing.'
echo
echo '  [-p | --plugin <plugin>]'
echo '        Plugin installation directory. (default: $CLARA_HOME/plugins/clas12)'
echo
echo '  [-s | --session <session>]'
echo '        The data processing session. (default: $USER)'
echo
echo '  [-m | --mode <mode>]'
echo '        The data processing mode. Accepts values = local, sqlite and farm (default: local)'
echo
echo '  [-i | --input_dir <inputDir>]'
echo '        The input directory where the files to be processed are located.'
echo '        (default: $CLARA_HOME/data/input)'
echo
echo '  [-o | --output_dir <outputDir>]'
echo '        The output directory where processed files will be saved.'
echo '        (default: $CLARA_HOME/data/output)'
echo
echo
echo '  [-t | --threads <maxThreads>]'
echo '        The maximum number of processing threads to be used per node. In case value = auto t=local-node processor count.'
echo '        (default: 36 for farm mode and 2 for the local mode))'
echo
echo '  [-f | --file-list <fileList>]'
echo '        Full path to the file containing the names of data-files to be processed. Note: actual files are located in the inputDir.'
echo '        (default: $CLARA_HOME/plugins/clas12/config/files.list)'
echo
echo '  [-y | --yaml <yamlComposition>]'
echo '        Full path to the file describing application service composition.'
echo '        (default: $CLARA_HOME/plugins/clas12/config/services.yaml)'
echo
echo '  [-ff | --farm-flavor <jlab> ]'
echo '        farm batch system. Accepts pbs and jlab. (default jlab)'
echo '  [-fl | --farm-loading-zone <local-staging-dir> ]'
echo '        Farm deployment only. Will stage input data set into the farm local directory. (default /scratch/pbs)'
echo '  [-fm | --farm-memory <farm memory>]'
echo '        Farm job memory request (in GB).'
echo '        (default: 70)'
echo '  [-ft | --farm-track <farm track>]'
echo '        Farm job track.'
echo '        (default: debug)'
echo '  [-fo | --farm-os <farm os>]'
echo '        Farm resource OS.'
echo '        (default: centos7)'
echo '  [-fc | --farm-cpu <farm cpu>]'
echo '        Farm resource core number request.'
echo '        (default: 72)'
echo '  [-fd | --farm-disk <farm disk>]'
echo '        Farm job disk space request (in GB).'
echo '        (default: 3)'
echo '  [-fw | --farm-time <farm time>]'
echo '        Farm job wall time request (in min).'
echo '        (default: 1440)'
echo '  [-fport | --farm-dpe-port <port number>]'
echo '        Farm DPE port number.'
echo '        (Do not set if in doubt.)'
}

######################################################################################################################
function list_parameters {
echo
echo '             Data processing parameters'
echo "--------------------------------------------------------"
echo
echo "  clara_home    = $CLARA_HOME"
echo "  plugin        = $PLUGIN"
echo "  input_dir     = $IN_DIR"
echo "  output_dir    = $OUT_DIR"
echo "  session       = $SESSION"
echo "  cores         = $THREAD_NUM"
echo "  services      = $SERVICE_YAML"
echo "  files         = $FILE_LIST"
echo "  description   = $DESCRIPTION"
echo "  farm_stage    = $FARM_LOADING_ZONE"
echo "  farm_memory   = $FARM_MEMORY"
echo "  farm_track    = $FARM_TRACK"
echo "  farm_os       = $FARM_OS"
echo "  farm_cpu      = $FARM_CPU"
echo "  farm_disk     = $FARM_DISK_SPACE"
echo "  farm_time     = $FARM_TIME"
echo "  farm_flavor   = $FARM_FLAVOR"
}

######################################################################################################################
# trap ctrl-c and call ctrl_c()
trap ctrl_c INT
function ctrl_c() {
echo "info: removing DPE at port = $DPE_PORT"
        $CLARA_HOME/bin/remove-dpe $DPE_PORT
}

######################################################################################################################
function command_exists () {
    type "$1" &> /dev/null ;
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
function set_default_parameters {
IN_DIR="$CLARA_HOME/data/input"
NODE_NUM="undefined"
OUT_DIR="$CLARA_HOME/data/output"
PLUGIN="$CLARA_HOME/plugins/clas12"
THREAD_NUM="2"
SERVICE_YAML="$PLUGIN/config/services.yaml"
FILE_LIST="$PLUGIN/config/files.list"
DPE_UP="false"
HOST=$(hostname)
USER=$(id -un)
FE_HOST="localhost"
SESSION="$USER"
MODE="local"

#farm default parameters
DESCRIPTION="clara"
FARM_LOADING_ZONE="undefined"
FARM_MEMORY="70"
FARM_TRACK="debug"
FARM_OS="centos7"
FARM_CPU="72"
FARM_DISK_SPACE="3"
FARM_TIME="1440"
FARM_DPE_ORC_DELAY="2"
FARM_FLAVOR="jlab"

}

######################################################################################################################
function run_farm {

# Farm specific local staging directory
{ if [ "$FARM_LOADING_ZONE" == "undefined" ]; then
if [ "$FARM_FLAVOR" == "jlab" ]; then
FARM_LOADING_ZONE="/scratch/pbs"
fi
fi
}

if [ "$PLUGIN" == "undefined" ]; then
PLUGIN="$CLARA_HOME/plugins/clas12"
fi

echo "-------- Farm Running Conditions ---------------"
if [ "$FARM_FLAVOR" == "jlab" ]; then
{
echo " Project            = clas12"
echo " Job name           = rec-$USER-$DESCRIPTION"
echo " Memory request     = $FARM_MEMORY GB"
echo " Track              = $FARM_TRACK"
echo " OS request         = $FARM_OS"
echo " Core request       = $FARM_CPU"
echo " Disk space request = $FARM_DISK_SPACE GB"
echo " Wall time request  = $FARM_TIME"
echo " Data staging area  = $FARM_LOADING_ZONE"
}
else {
echo " Project            = clas12"
echo " Job name           = rec-$USER-$DESCRIPTION"
#echo " Memory request     = $FARM_MEMORY GB"
#echo " Track              = $FARM_TRACK"
#echo " OS request         = $FARM_OS"
echo " Core request       = $FARM_CPU"
echo " Disk space request = 3145728 KB"
echo " Wall time request  = 24:00:00"
}
fi
echo "------------------------------------------"
echo

a="setenv CLARA_HOME $CLARA_HOME"
b="$a ; $CLARA_HOME/bin/remove-dpe"
if [ "$FARM_DPE_PORT" == "undefined" ]; then
rcCmd="$a ; $CLARA_HOME/bin/etc/f-clara.sh $IN_DIR $OUT_DIR $SERVICE_YAML $FILE_LIST $PLUGIN $DESCRIPTION $FARM_CPU $SESSION $FARM_LOADING_ZONE"
else
rcCmd="$a ; $CLARA_HOME/bin/etc/f-clara.sh $IN_DIR $OUT_DIR $SERVICE_YAML $FILE_LIST $PLUGIN $DESCRIPTION $FARM_CPU $SESSION $FARM_LOADING_ZONE $FARM_DPE_PORT"
fi

# Create orchestrator job submission script
# ------------- JLAB auger script creation and submission ------------
if [ "$FARM_FLAVOR" == "jlab" ]; then
{
echo "PROJECT: clas12"
echo "JOBNAME: rec-$USER-$DESCRIPTION"
echo "MEMORY: $FARM_MEMORY GB"
echo "TRACK: $FARM_TRACK"
echo "OS: $FARM_OS"
echo "CPU: $FARM_CPU"
echo "DISK_SPACE: $FARM_DISK_SPACE GB"
echo "TIME: $FARM_TIME"

echo "COMMAND: $rcCmd"

} > $PLUGIN/config/clara_p.jsub
sleep 3

# Submit auger job request
if command_exists jsub
 then
    jsub $PLUGIN/config/clara_p.jsub
 else
    echo " Error: can not run farm job from this node = $HOST"
fi

# ------------------------------------------------------------
else
# ------------- PBS script creation and submission -----------
{
echo "#!/bin/csh"
echo "#PBS -N rec-$USER-$DESCRIPTION"
#echo "#PBS -q $FARM_TRACK"
echo "#PBS -A clas12"
#echo "#PBS -M gurjyan@jlab.org"
echo "#PBS -S /bin/csh"
echo "#PBS -l nodes=1:ppn=$FARM_CPU"
#echo "#PBS -l arch=$FARM_OS"
echo "#PBS -l file=3145728kb"
echo "#PBS -l walltime=24:00:00"

echo $rcCmd

} > $PLUGIN/config/clara_p.pbs
sleep 3

# Submit auger job request
if command_exists qsub
 then
    qsub $PLUGIN/config/clara_p.pbs
 else
    echo " Error: can not run farm job from this node = $HOST"
fi
fi
}



# main program
set_default_parameters

print_logo

while :
do
    case "$1" in
      -d | --description)
	  DESCRIPTION="$2"
	  shift 2
	  ;;
      -f | --files-list)
	  FILE_LIST="$2"
	  shift 2
	  ;;
      -h | --help)
	  display_help  # function call
	  exit 0
	  ;;
      -i | --input-dir)
	  IN_DIR="$2"
	  shift 2
	  ;;
      -m | --mode)
      {
      if [ "$2" == "farm" ] || [ "$2" == "sqlite" ] ; then
	      MODE="$2"
	  fi
	  }
	  shift 2
	  ;;
      -o | --output-dir)
	  OUT_DIR="$2"
	  shift 2
	  ;;
      -p | --plugin)
	  PLUGIN="$2"
	  shift 2
	  ;;
      -s | --session)
	  SESSION="$2"
	  shift 2
	  ;;
      -t | --threads)
	  THREAD_NUM="$2"

	  if [ "$THREAD_NUM" == "auto" ] ; then
	  THREAD_NUM=`getconf _NPROCESSORS_ONLN`
	  fi

	  shift 2
	  ;;
      -y | --yaml)
	  SERVICE_YAML="$2"
	  shift 2
	  ;;
      -ff | --farm-flavor)
	  FARM_FLAVOR="$2"
	  shift 2
	  ;;
      -fl | --farm-loading-zone)
	  FARM_LOADING_ZONE="$2"
	  shift 2
	  ;;
      -fm | --farm-memory)
	  FARM_MEMORY="$2"
	  shift 2
	  ;;
      -ft | --farm-track)
	  FARM_TRACK="$2"
	  shift 2
	  ;;
      -fo | --farm-os)
	  FARM_OS="$2"
	  shift 2
	  ;;
      -fc | --farm-cpu)
	  FARM_CPU="$2"
	  shift 2
	  ;;
      -fd | --farm-disk)
	  FARM_DISK_SPACE="$2"
	  shift 2
	  ;;
      -fw | --farm-time)
	  FARM_TIME="$2"
	  shift 2
	  ;;
      -fdod | --farm-dpe-orc-delay)
	  FARM_DPE_ORC_DELAY="$2"
	  shift 2
	  ;;
      -fport | --farm-dpe-port)
	  FARM_DPE_PORT="$2"
	  shift 2
	  ;;

     *)  # No more options
	  break
	  ;;
    esac
done

list_parameters

# ----------------- farm deployment ----------------------
if [ "$MODE" == "farm" ]; then
    run_farm

# ----------------- Local deployment ----------------------
else
	      # define private clara port
          define_dpe_port

          # define this host IP
          define_host_ip

          # define local DPE name
          FENAME=$IP%$DPE_PORT$lang

	     $CLARA_HOME/bin/etc/l-clara.sh $IN_DIR $OUT_DIR $SERVICE_YAML $FILE_LIST $PLUGIN $DESCRIPTION $THREAD_NUM $SESSION $DPE_UP $DPE_PORT $FENAME

fi
$CLARA_HOME/bin/remove-dpe $DPE_PORT
