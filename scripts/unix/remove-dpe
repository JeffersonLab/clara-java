#!/usr/bin/env bash
# date 1.13.17

a=`ps -ef | grep 'port '$1 | grep -v grep | grep $USER | awk '{print $2}'`

if ! [ -z "$a" ]; then
for f in $a;
do
kill -9 $f
done
fi


