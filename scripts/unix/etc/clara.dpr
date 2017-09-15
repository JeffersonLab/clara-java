#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 1.13.17

#MALLOC_ARENA_MAX=1; export MALlOC_ARENA_MAX

if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME is not defined. Exiting..."
    exit
fi

DPE_PORT=
IP=127.0.0.1
lang="_java"
FENAME=

unset CCDB_DATABASE

set -o errexit -o nounset -o pipefail

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
function display_options_help {
echo
echo '                    Customization Options'
echo "-------------------------------------------------------------------"
echo
echo '  [-d | --description <description>]'
echo '        A single string (no spaces) describing a data processing.'
echo
echo '  [-p | --plugin <plugin>]'
echo '        Plugin installation directory. (default: $CLARA_HOME/plugins/clas12)'
echo
echo '  [-s | --author <author>]'
echo '        The data processing author. (default: $USER)'
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
#echo
echo '  [-t | --threads <maxThreads>]'
echo '        The maximum number of processing threads to be used per node.'
echo '        In case value = auto all system cores will be used.'
echo '        (default: 2))'
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

######################################################################################################################
function display_main_help {
echo
echo '               Help'
echo "----------------------------------------"
echo
echo '  [-h | --help | help  <set>]'
echo '        Parameter settings'
echo
echo '  [-h | --help | help  <edit>]'
echo '        Edit data processing conditions'
echo
echo '  [-h | --help | help  <run>]'
echo '        Start data processing'
echo
echo '  [-h | --help | help  <monitor>]'
echo '        Monitor data processing'
}

######################################################################################################################
function display_edit_help {
echo
echo '               Edit'
echo "----------------------------------------"
echo
echo '  [edit  <composition>]'
echo '        Edit application service-based composition'
echo
echo '  [edit  <files>]'
echo '        Edit input file list'
echo
echo '  [edit <>]'
echo '        Edit help'
}

######################################################################################################################
function display_monitor_help {
echo
echo '               Monitor'
echo "----------------------------------------"
echo
echo '  [monitor  <composition>]'
echo '        Show application service-based composition.'
echo
echo '  [monitor  <files>]'
echo '        Show input file list.'
echo
echo '  [monitor  <idir>]'
echo '        List input files directory.'
echo
echo '  [monitor  <odir>]'
echo '        List output files directory.'
echo
echo '  [monitor  <params>]'
echo '        Show data processing configuration parameters.'
echo
echo '  [monitor  <logdir>]'
echo '        Show data processing log of DPE.'
echo
echo '  [monitor  <logdpe>]'
echo '        Show data processing log of DPE.'
echo
echo '  [monitor  <logco>]'
echo '        Show data processing log of cloud orchestrator.'
echo
echo '  [monitor  <jjobstat>]'
echo '        Show JLab farm auger-submitted job statuses.'
echo '        Works on interactive fam nodes only.'
echo
echo '  [monitor  <pjobstat>]'
echo '        Show PBS controlled farm submitted job statuses.'
echo '        Works on interactive fam nodes only.'
echo
echo '  [monitor <>]'
echo '        Monitor help.'
}

######################################################################################################################
function display_run_help {
echo
echo '               Run'
echo "----------------------------------------"
echo
echo '  [run  <local>]'
echo '        Run data processing application on a local node'
echo
echo '  [run  <farm>]'
echo '        Run data processing application on a farm'
echo '        Note: Clara distribution and data input/output directories'
echo '              must be visible to farm nodes.'
echo
echo '  [run <>]'
echo '        Run help'
}

######################################################################################################################
function display_reset_help {
echo
echo '               Reset'
echo "----------------------------------------"
echo
echo '  [reset  <param>]'
echo '        Reset parameters to their default values'
echo
echo '  [reset  <dpe>]'
echo '        Stop running DPEs'
echo
echo '  [reset <>]'
echo '        Reset help'
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
echo "  author       = $SESSION"
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
IN_DIR="$CLARA_HOME/data/in"
NODE_NUM="undefined"
OUT_DIR="$CLARA_HOME/data/out"
PLUGIN="$CLARA_HOME/plugins/clas12"
THREAD_NUM="2"
SERVICE_YAML="$PLUGIN/config/services.yaml"
FILE_LIST="$PLUGIN/config/files.list"
DPE_UP="false"
HOST=$(hostname)
USER=$(id -un)
FE_HOST="localhost"
SESSION="$USER"

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
#

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
rcCmd="$a ; $CLARA_HOME/bin/etc/f-clara.sh $IN_DIR $OUT_DIR $SERVICE_YAML $FILE_LIST $PLUGIN $DESCRIPTION $FARM_CPU $SESSION $FARM_LOADING_ZONE"

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

######################################################################################################################
function decode_user_input () {

local opt="$1"
local val1="$2"
local val2="$3"

    case "$opt" in
# ---------------- configuring ------------------
      -h | --help | help)
      if [ "$val1" == "set" ]; then
	    display_options_help
	  elif [ "$val1" == "edit" ]; then
	    display_edit_help
	  elif [ "$val1" == "monitor" ]; then
	    display_monitor_help
	  elif [ "$val1" == "run" ]; then
	    display_run_help
	  elif [ "$val1" == "reset" ]; then
	    display_reset_help
	  else
	    display_main_help
	  fi
	  ;;
      -d | --description)
      if ! [ -z "$val1" ]; then
	  DESCRIPTION="$val1"
	  fi
	  ;;
      -f | --files-list)
      if ! [ -z "$val1" ]; then
	  FILE_LIST="$val1"
	  fi
	  ;;
      -i | --input-dir)
      if ! [ -z "$val1" ]; then
	  IN_DIR="$val1"
	  fi
	  ;;
#      -n | --nodes)
#	  NODE_NUM="$val1"
#	  ;;
      -o | --output-dir)
      if ! [ -z "$val1" ]; then
	  OUT_DIR="$val1"
	  fi
	  ;;
      -p | --plugin)
      if ! [ -z "$val1" ]; then
	  PLUGIN="$val1"
	  fi
	  ;;
      -s | --author)
      if ! [ -z "$val1" ]; then
	  SESSION="$val1"
	  fi
	  ;;
      -t | --threads)
      if ! [ -z "$val1" ]; then
	  THREAD_NUM="$val1"

	  if [ "$THREAD_NUM" == "auto" ] ; then
	  THREAD_NUM=`getconf _NPROCESSORS_ONLN`
	  fi
	  fi
	  ;;
      -y | --yaml)
      if ! [ -z "$val1" ]; then
	  SERVICE_YAML="$val1"
	  fi
	  ;;
      -ff | --farm-flavor)
      if ! [ -z "$val1" ]; then
	  FARM_FLAVOR="$val1"
	  fi
	  ;;
      -fl | --farm-loading-zone)
      if ! [ -z "$val1" ]; then
	  FARM_LOADING_ZONE="$val1"
	  fi
	  ;;
      -fm | --farm-memory)
      if ! [ -z "$val1" ]; then
	  FARM_MEMORY="$val1"
	  fi
	  ;;
      -ft | --farm-track)
      if ! [ -z "$val1" ]; then
	  FARM_TRACK="$val1"
	  fi
	  ;;
      -fo | --farm-os)
      if ! [ -z "$val1" ]; then
	  FARM_OS="$val1"
	  fi
	  ;;
      -fc | --farm-cpu)
      if ! [ -z "$val1" ]; then
	  FARM_CPU="$val1"
	  fi
	  ;;
      -fd | --farm-disk)
      if ! [ -z "$val1" ]; then
	  FARM_DISK_SPACE="$val1"
	  fi
	  ;;
      -fw | --farm-time)
      if ! [ -z "$val1" ]; then
	  FARM_TIME="$val1"
	  fi
	  ;;
      -fdod | --farm-dpe-orc-delay)
      if ! [ -z "$val1" ]; then
	  FARM_DPE_ORC_DELAY="$val1"
	  fi
	  ;;
# ------------------ edit ---------------
      edit)
      if ! [ -z "$val1" ]; then
	   if [ "$val1" == "composition" ]; then
	     vi $SERVICE_YAML
	   elif [ "$val1" == "files" ]; then
	     vi $FILE_LIST
	   elif [ "$val1" == "-h" ]; then
	     display_edit_help
	   else
	     display_edit_help
	     fi
	  fi
	  ;;
# ------------------ monitor ---------------
      monitor)
      if ! [ -z "$val1" ]; then
	   if [ "$val1" == "composition" ]; then
	     cat $SERVICE_YAML
	   elif [ "$val1" == "files" ]; then
	     cat $FILE_LIST
	   elif [ "$val1" == "idir" ]; then
	     ls -l $IN_DIR
	   elif [ "$val1" == "odir" ]; then
	     ls -l $OUT_DIR
	   elif [ "$val1" == "params" ]; then
	     list_parameters
	   elif [ "$val1" == "jjobstat" ]; then
	    if command_exists jobstat
          then
	        jobstat -u $USER
	      else
	       echo "Error: farm operations are not permitted from this node = $HOST"
	    fi
	   elif [ "$val1" == "pjobstat" ]; then
	    if command_exists qstat
          then
	        qstat -u $USER
	      else
	       echo "Error: farm operations are not permitted from this node = $HOST"
	    fi
	   elif [ "$val1" == "logdir" ]; then
	     ls -l $CLARA_HOME/log
	   elif [ "$val1" == "logdpe" ]; then
	    LOG_FILE="$CLARA_HOME/log/*-$SESSION-$DESCRIPTION-jfe.log"
	    for k in $LOG_FILE; do
	   if [ -f "$k" ]
         then
	       more $k
       fi
       done
	   elif [ "$val1" == "logco" ]; then
	    LOG_FILE="$CLARA_HOME/log/*-$SESSION-$DESCRIPTION-co.log"
	    for k in $LOG_FILE; do
	   if [ -f "$k" ]
         then
	       more $k
       fi
       done
	   elif [ "$val1" == "-h" ]; then
	     display_monitor_help
	   else
	     display_monitor_help
	  fi
	  fi
	  ;;
# ------------------ running ---------------
      run)
      if ! [ -z "$val1" ]; then
	   if [ "$val1" == "local" ]; then

	    if [ "$DPE_UP" == "false" ]; then
	      # define private clara port
          define_dpe_port

          # define this host IP
          define_host_ip

          # define local DPE name
          FENAME=$IP%$DPE_PORT$lang
        fi

	     $CLARA_HOME/bin/etc/l-clara.sh $IN_DIR $OUT_DIR $SERVICE_YAML $FILE_LIST $PLUGIN $DESCRIPTION $THREAD_NUM $SESSION $DPE_UP $DPE_PORT $FENAME
	     DPE_UP="true"
	   elif [ "$val1" == "farm" ]; then
	      run_farm
	   elif [ "$val1" == "-h" ]; then
	     display_run_help
	   else
	     display_run_help
       fi
	  fi
	  ;;
# ------------------ resetting ---------------
      reset)
       if ! [ -z "$val1" ]; then
	   if [ "$val1" == "param" ]; then
	     set_default_parameters
	   elif [ "$val1" == "dpe" ]; then
      DPE_UP="false"
      $CLARA_HOME/bin/remove-dpe $DPE_PORT
	   elif [ "$val1" == "-h" ]; then
	     display_reset_help
	   else
	     display_reset_help
       fi
	  fi
	  ;;

      exit )
	  printf "bye... \n"
	  $CLARA_HOME/bin/remove-dpe $DPE_PORT
	  exit
	  ;;

     *)  # No more options
     if  ! [ -z "$opt" ]; then
	  printf "unknown option\n"
	  fi
	  ;;
    esac
}

######################################################################################################################
function main_loop() {
#    local cmd=
    local char=
    local string=
    local -a history=( )
    local -i histindex=0

    printf "\n"
    printf "clara>"

    # Read one character at a time
    while IFS= read -r -n 1 -s char
    do

        if [ "$char" == $'\x1b' ] # \x1b is the start of an escape sequence
        then
            # Get the rest of the escape sequence (3 characters total)
            while IFS= read -r -n 2 -s rest
            do
                char+="$rest"
                break
            done
        fi

        if [ "$char" == $'\x1b[A' ]
        then
            # Up
            if [ $histindex -gt 0 ]
            then
                histindex+=-1
                echo -ne "\r\033[Kclara> ${history[$histindex]}"
                string=${history[$histindex]}
            fi
        elif [ "$char" == $'\x1b[B' ]
        then
            # Down
            if [ $histindex -lt $((${#history[@]} - 1)) ]
            then
                histindex+=1
                echo -ne "\r\033[Kclara> ${history[$histindex]}"
                string=${history[$histindex]}
            fi
        elif [ "$char" == $'\x7f' ]
        then
            # Backspace
             string=
             echo -ne "\033[0K\r"
             echo -ne "                                                       "
             echo -ne "\033[0K\r"
             echo -ne "clara>"
        elif [ -z "$char" ]
        then
            # Newline
            printf "\n"
                if ! [ -z "$string" ]
                then
                  history+=( "$string" )
                  cmd=${history[$histindex]}

                if ! [ -z "$cmd" ]
                then
                    IFS=' '
                    arrCmd=($cmd)
                    unset IFS;

                    # actual decoding and execution. first check number of cmd line parameters passed
                    cmdSize=${#arrCmd[@]}
                     if [ $cmdSize == 1 ]
                     then
                       decode_user_input ${arrCmd[0]} "udf" "udf"
                     elif [ $cmdSize == 2 ]
                     then
                       decode_user_input ${arrCmd[0]} ${arrCmd[1]} "udf"
                     elif [ $cmdSize == 3 ]
                     then
                       decode_user_input ${arrCmd[0]} ${arrCmd[1]} ${arrCmd[2]}
                    else
                       decode_user_input "udf" "udf" "udf"
                    fi

                fi
            histindex=${#history[@]}
            string=
          fi
            printf "clara>"
        else
            echo -n "$char"
            string+="$char"
        fi
    done
}

# ------- program start----------------

# define defaults before the loop
set_default_parameters

print_logo

main_loop
