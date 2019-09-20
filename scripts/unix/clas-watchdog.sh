#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 10.20.19

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
valid=true

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
while [[ ${valid} ]]
do

# Get cpu usage
get_dpe_cpu_usage

# check if dpe is done then exit.
# This is the case when cpu_usage returns an empty string.
if [[ -z ${cpu_usage} ]]; then
exit 0
fi

float_compare $cpu_usage $cpu_idle

result="${__FUNCTION_RETURN}"


# Check if cpu_usage is less than 1%
if [[ ${result} -eq 0 ]] ; then

# Log the error
echo `date`  "clara-wd:Error     DPE %CPU = ${cpu_usage} DPE_PID = ${dpe_pid} ORCH_PID = ${orch_pid} timeout = ${timeout}"

time=$((time + 1))

else
time=0

fi

# Check to see if we are not using CPU for timeout seconds.
if (( $time > $timeout )); then
echo `date`  "clara-wd:SevereError  Stop the data-processing... "
kill -9 ${dpe_pid}
kill -9 ${orch_pid}
exit 1
# Create a new file.list and relaunch clara
# Not implemented
time=0
fi

sleep 10
done

