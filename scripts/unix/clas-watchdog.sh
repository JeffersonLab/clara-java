#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 10.21.19

#################################
# Parameters
#################################

# 1 = DPE process pid
# 2 = Orchestrator process pid
# 3 = timeout

# Initialization
dpe_pid=$1
orch_pid=$2
timeout=$3

time=0
# Below 10% cpu_usage DPE process will be considered idled
cpu_idle=10.00

echo `date`  "clara-wd:Info  Monitoring DPE_PID=$1  ORCH_PID=$2  TIMEOUT=$3"

#################################
# Function echo to stderr
#################################
echoerr() {
cat <<< "$@" 1>&2;
}

#################################
# Function that returns 1 if the
# first float number is bigger
# than the second float number
#################################
float_compare () {
    number1="$1"
    number2="$2"

    [ ${number1%.*} -eq ${number2%.*} ] && [ ${number1#*.} \> ${number2#*.} ] || [ ${number1%.*} -gt ${number2%.*} ];
    result=$?
    if [ "$result" -eq 0 ]; then result=1; else result=0; fi

    __FUNCTION_RETURN="${result}"
}

#################################
# Function to get the DPE process
# CPU usage. It will measure the
# average of 10 snapshots of the
# cpu_usage in a whole of 1 second
# that provides a good and fast
# accurate result of what is
# happening in the very moment.
#################################
function get_dpe_cpu_usage() {
nTimes=10;
delay=0.1;
strCalc=`top -d $delay -b -n $nTimes -p $dpe_pid \
  |grep $dpe_pid \
  |sed -r -e "s;\s\s*; ;g" -e "s;^ *;;" \
  |cut -d' ' -f9 \
  |tr '\n' '+' \
  |sed -r -e "s;(.*)[+]$;\1;" -e "s/.*/scale=2;(&)\/$nTimes/"`;
cpu_usage=`echo "$strCalc" |bc -l`
}

#################################
# Main loop
#################################
while [[ true ]]
do

    # Check if DPE is running
    if ! ps -p ${dpe_pid} > /dev/null; then
        echoerr `date`   'clara-wd:SevereError   DPE is not running, exiting...'
        kill -9 ${orch_pid} >& /dev/null
        kill -9 ${dpe_pid} >& /dev/null
        exit 13
    fi

    # Check if Orchestrator is running
    if ! ps -p ${orch_pid} > /dev/null; then
        sleep 10
        if ! ps -p ${orch_pid} > /dev/null; then
            echo `date` 'clara-wd:Warning   ORCH is not running, exiting normally.'
            kill -9 ${dpe_pid} >& /dev/null
            kill -9 ${orch_pid} >& /dev/null
            exit 0
        fi
    fi

    # Get cpu usage
    get_dpe_cpu_usage

    # if DPE cpu_usage is defined
    if ! [[ -z ${cpu_usage} ]]; then
    
        float_compare $cpu_usage $cpu_idle
        
        result="${__FUNCTION_RETURN}"
        
        # Check if cpu_usage is less than 1%
        if [[ ${result} -eq 0 ]] ; then
            # Log the error
            echoerr `date`  "clara-wd:Error     DPE %CPU = ${cpu_usage} DPE_PID = ${dpe_pid} ORCH_PID = ${orch_pid} timeout = ${timeout}"
            time=$((time + 1))
        else
            time=0
        fi
        
        # Check to see if we are not using CPU for timeout seconds.
        if (( $time > $timeout )); then
            echoerr `date`  "clara-wd:SevereError  Stop the data-processing... "
            kill -9 ${dpe_pid}
            kill -9 ${orch_pid}
            exit 1
            # Create a new file.list and relaunch clara
            # Not implemented
            time=0
        fi
   
    else
        echoerr `date`  "clara-wd:Error  DPE CPU usage undefined. "
    fi
    
    sleep 10

done

# we should never get here:
kill -9 ${dpe_pid}
kill -9 ${orch_pid}
exit 2

