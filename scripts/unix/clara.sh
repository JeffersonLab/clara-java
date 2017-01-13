#!/usr/bin/env bash
# author vhg
# date 1.13.17

if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME is not defined. Exiting..."
    exit
fi

# ------------- Parameter default settings ---------
CL_HOME="undefined"
IN_DIR="undefined"
J_HOME="undefined"
MODE="undefined"
NODE_NUM="undefined"
OUT_DIR="undefined"
PLUGIN="undefined"
SESSION="undefined"
THREAD_NUM="undefined"
SERVICE_YAML="undefined"
FILE_LIST="undefined"
DPE_UP="false"

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
# ------------------------------------------------

unset CCDB_DATABASE

printf "  ________   ____         ________   _________    ________\n"
printf " /        \ |    |       /        | |         \  /        | 4.3.0\n"
printf "|    |    | |    |      |   _._   | |    |    | |   _._   |\n"
printf "|    |____| |    |      |    |    | |        /  |    |    |\n"
printf "|    |    | |    |____  |    |    | |    .   -| |    |    |\n"
printf "|    |    | |         | |    |    | |    |    | |    |    |\n"
printf "|________/  |_________| |____|____| |____|____| |____|____|\n"

function update_parameters {

# CLARA_HOME
{ if [ "$CL_HOME" == "undefined" ]; then

if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME is not defined. Exiting..."
    exit
fi

CL_HOME=$CLARA_HOME

fi }

# Input data directory
if [ "$IN_DIR" == "undefined" ]; then
IN_DIR="$CLARA_HOME/data/in"
fi

# JAVA_HOME
if [ "$J_HOME" == "undefined" ]; then

OS="`uname`"
case $OS in
  'Linux')
IP=$(host `hostname` | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b")
  if [ -z "$IP" ]; then
  IP=127.0.0.1
  fi

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

  IP=$(ipconfig getifaddr en0)
  if [ -z "$IP" ]; then
  IP=$(ipconfig getifaddr en1)
  fi

if [ -z "$IP" ]; then
 IP=127.0.0.1
fi

  J_HOME=$CLARA_HOME/jre/macosx-64/jre1.8.0_112.jre/Contents/Home
    ;;
  *) ;;
esac
fi

# Running mode
if [ "$MODE" == "undefined" ]; then
MODE=local
fi

# Number of nodes in case of farm deployment
if [ "$NODE_NUM" == "undefined" ]; then
NODE_NUM=1
fi

# Output directory
if [ "$OUT_DIR" == "undefined" ]; then
OUT_DIR="$CLARA_HOME/data/out"
fi

# User service plugin
if [ "$PLUGIN" == "undefined" ]; then
PLUGIN="$CLARA_HOME/plugins/clas12"
fi

# Data processing session
if [ "$SESSION" == "undefined" ]; then
SESSION="$USER"
fi

# Number of parallel threads
if [ "$THREAD_NUM" == "undefined" ]; then
if [ "$MODE" = "farm" ]; then
THREAD_NUM=72
else
THREAD_NUM=2
fi
fi

export JAVA_HOME="$J_HOME"
export CLARA_HOME="$CL_HOME"
export CLAS12DIR="$PLUGIN"
#export CCDB_DATABASE="etc/data/database/clas12database.sqlite"
export PATH=$PATH:$JAVA_HOME/bin:$CLARA_HOME/bin
export CLASSPATH="$CLARA_HOME/lib/*:$CLAS12DIR/lib/clas/*:$CLAS12DIR/lib/services/*"

# Composition yaml file
if [ "$SERVICE_YAML" == "undefined" ]; then
SERVICE_YAML="$CLAS12DIR/config/services.yaml"
fi

# List of files to be processed
if [ "$FILE_LIST" == "undefined" ]; then
FILE_LIST="$CLAS12DIR/config/files.list"
fi

# Farm specific local staging directory
if [ "$FARM_LOADING_ZONE" == "undefined" ]; then
if [ "$FARM_FLAVOR" == "jlab" ]; then
FARM_LOADING_ZONE="/scratch/pbs"
fi
fi

}

function display_options_help {
echo
echo '                    Customization Options'
echo "-------------------------------------------------------------------"
echo
echo '  [-j | --java_home <java_home>]'
echo '        JDK/JRE installation directory. (default: $JAVA_HOME)'
echo
echo '  [-c | --clara_home <clara_home>]'
echo '        CLARA installation directory. (default: $CLARA_HOME)'
echo '  [-d | --description <description>]'
echo '        A single string (no spaces) describing a data processing.'
echo
echo '  [-p | --plugin <plugin>]'
echo '        Plugin installation directory. (default: $CLARA_HOME/plugins/clas12)'
echo
echo '  [-s | --session <session>]'
echo '        The data processing session. (default: $USER)'
echo
echo '  [-m | --mode <mode>]'
echo '        The data processing mode. Accepted values = local, sqlite and farm'
echo '        (default: local)'
echo
echo '  [-i | --input_dir <inputDir>]'
echo '        The input directory where the files to be processed are located.'
echo '        (default: $CLARA_HOME/data/in)'
echo
echo '  [-o | --output_dir <outputDir>]'
echo '        The output directory where processed files will be saved.'
echo '        (default: $CLARA_HOME/data/out)'
echo
#echo '  [-n | --nodes <maxNodes>]'
#echo '        The maximum number of processing nodes to be used. Farm mode only. (default: 1)'
echo
echo '  [-t | --threads <maxThreads>]'
echo '        The maximum number of processing threads to be used per node.'
echo '        In case value = auto all system cores will be used.'
echo '        (default: 36 for farm mode and 2 for the local mode))'
echo
echo '  [-f | --file-list <fileList>]'
echo '        Full path to the file containing the names of data-files to be '
echo '        processed. Note: actual files are located in the inputDir.'
echo '        (default: $CLARA_HOME/plugins/clas12/config/files.list)'
echo
echo '  [-y | --yaml <yamlComposition>]'
echo '        Full path to the file describing application service composition.'
echo '        (default: $CLARA_HOME/plugins/clas12/config/services.yaml)'
echo
echo '  [-ff | --farm-flavor <jlab> ]'
echo '        farm batch system. Accepts pbs and jlab. (default jlab)'
echo
echo '  [-fl | --farm-loading-zone <local-staging-dir> ]'
echo '         Will stage input data set into the farm local directory. '
echo '         (default /scratch/pbs)'
echo
echo '  [-fm | --farm-memory <farm memory>]'
echo '         Farm job memory request (in GB).'
echo '         (default: 70)'
echo
echo '  [-ft | --farm-track <farm track>]'
echo '         Farm job track.'
echo '         (default: debug)'
echo
echo '  [-fo | --farm-os <farm os>]'
echo '         Farm resource OS.'
echo '         (default: centos7)'
echo
echo '  [-fc | --farm-cpu <farm cpu>]'
echo '         Farm resource core number request.'
echo '         (default: 72)'
echo
echo '  [-fd | --farm-disk <farm disk>]'
echo '         Farm job disk space request (in GB).'
echo '         (default: 3)'
echo
echo '  [-fw | --farm-time <farm time>]'
echo '         Farm job wall time request (in min).'
echo '         (default: 1440)'
}

function display_run_help {
echo
echo '                            Run Help'
echo "-------------------------------------------------------------------"
echo
echo '  [run-local]'
echo '        Starts data processing with defined parameter set on local node.'
echo
echo '  [run-farm]'
echo '        Starts a data processing job on a local farm'
echo
echo '  [params]'
echo '        Lists data processing configuration options'
echo
echo '  [reset]'
echo '        Reset to default parameter settings'
echo
echo '  [stop-dpe]'
echo '        Stops currently running DPEs on a local node.'
}

function list_parameters {
echo
echo '             Data processing parameters'
echo "--------------------------------------------------------"
echo
echo "  java_home     = $JAVA_HOME"
echo "  clara_home    = $CLARA_HOME"
echo "  plugin        = $PLUGIN"
echo "  input_dir     = $IN_DIR"
echo "  output_dir    = $OUT_DIR"
echo "  session       = $SESSION"
echo "  cores         = $THREAD_NUM"
echo "  services      = $SERVICE_YAML"
echo "  files         = $FILE_LIST"
echo "  mode          = $MODE"
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

function set_default_parameters {
CL_HOME="undefined"
IN_DIR="undefined"
J_HOME="undefined"
MODE="undefined"
NODE_NUM="undefined"
OUT_DIR="undefined"
PLUGIN="undefined"
SESSION="undefined"
THREAD_NUM="undefined"
SERVICE_YAML="undefined"
FILE_LIST="undefined"
DPE_UP="false"

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

update_parameters
}
#--------------------------------------

# ------- program loop ----------------
# define defaults before the loop
update_parameters

while :
do
printf "\n"
printf "clara"
echo -en "\033[5C>"
read opt value

    case "$opt" in
# ---------------- configuring ------------------
      -c | --clara-home)
	  CL_HOME="$value"
	  export CLARA_HOME="$CL_HOME"
	  ;;
      -d | --description)
	  DESCRIPTION="$value"
	  ;;
      -f | --files-list)
	  FILE_LIST="$value"
	  ;;
      -h | --help)
      if [ "$value" == "param" ]; then
	    display_options_help
	  else
	    display_run_help
	  fi
	  ;;
      -i | --input-dir)
	  IN_DIR="$value"
	  ;;
      -j | --java-home)
	  J_HOME="$value"
	  ;;
      -m | --mode)
      {
      if [ "$value" == "farm" ] || [ "$value" == "sqlite" ] ; then
	      MODE="$value"
	  fi
	  }
	  ;;
#      -n | --nodes)
#	  NODE_NUM="$value"
#	  ;;
      -o | --output-dir)
	  OUT_DIR="$value"
	  ;;
      -p | --plugin)
	  PLUGIN="$value"
	  ;;
      -s | --session)
	  SESSION="$value"
	  ;;
      -t | --threads)
	  THREAD_NUM="$value"

	  if [ "$THREAD_NUM" == "auto" ] ; then
	  THREAD_NUM=`getconf _NPROCESSORS_ONLN`
	  fi

	  ;;
      -y | --yaml)
	  SERVICE_YAML="$value"
	  ;;
      -ff | --farm-flavor)
	  FARM_FLAVOR="$value"
	  ;;
      -fl | --farm-loading-zone)
	  FARM_LOADING_ZONE="$value"
	  ;;
      -fm | --farm-memory)
	  FARM_MEMORY="$value"
	  ;;
      -ft | --farm-track)
	  FARM_TRACK="$value"
	  ;;
      -fo | --farm-os)
	  FARM_OS="$value"
	  ;;
      -fc | --farm-cpu)
	  FARM_CPU="$value"
	  ;;
      -fd | --farm-disk)
	  FARM_DISK_SPACE="$value"
	  ;;
      -fw | --farm-time)
	  FARM_TIME="$value"
	  ;;
      -fdod | --farm-dpe-orc-delay)
	  FARM_DPE_ORC_DELAY="$value"
	  ;;
# ------------------ running ---------------
      reset)
	  set_default_parameters
	  ;;
      params)
	  list_parameters
	  ;;
      run-local)
      run_local
      DPE_UP="true"
	  ;;
      stop-dpe)
      DPE_UP="false"
      $CLARA_HOME/bin/remove-dpe
	  ;;
      run-farm)
      run_farm
	  ;;
      exit )
	  printf "bye... \n"
	  $CLARA_HOME/bin/remove-dpe
	  exit
	  ;;

     *)  # No more options
	  printf "unknown option\n"
	  ;;
    esac
done

