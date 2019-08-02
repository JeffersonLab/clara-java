#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 7.27.19

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
cpu_idle="1.00"
valid=true

# main loop
while [[ ${valid} ]]
do

# get cpu usage
cpu_usage=`top -b -n 1 -p ${dpe_pid} | awk -v OFS="," '$1+0>0 { print $9 fflush() }'`


# check if dpe is done then exit.
# This is the case when cpu_usage returns an empty string.
if [[ -z ${cpu_usage} ]]; then
exit 0
fi

# check if cpu_usage is less than 1%
if [[ ${cpu_usage} < ${cpu_idle} ]] ; then
echo `date`  "clara-wd:Error        DPE %CPU = ${cpu_usage} "

time=$((time + 1))
else
time=0
fi

# check to see if we are not using CPU for timeout seconds.
if (( $time > $timeout )); then
echo `date`  "clara-wd:SevereError  Stop the data-processing... "
kill -9 ${dpe_pid}
kill -9 ${orch_pid}
exit 1
# create a new file.list and relaunch clara
# not implemented
time=0
fi

sleep 10
done

