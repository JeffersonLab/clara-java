#!/usr/bin/env bash

a=`ps -ef | grep j_cloud | grep -v grep | grep $USER | awk '{print $2}'`
if ! [ -z "$a" ]; then
for f in $a;
do
kill -9 $f
done
fi

a=`ps -ef | grep CloudOrchestrator | grep -v grep | grep $USER | awk '{print $2}'`
if ! [ -z "$a" ]; then
for f in $a;
do
kill -9 $f
done
fi

a=`ps -ef | grep j_dpe | grep -v grep | grep $USER | awk '{print $2}'`
if ! [ -z "$a" ]; then
for f in $a;
do
kill -9 $f
done
fi

a=`ps -ef | grep Dpe | grep -v grep | grep $USER | awk '{print $2}'`
if ! [ -z "$a" ]; then
for f in $a;
do
kill -9 $f
done
fi
