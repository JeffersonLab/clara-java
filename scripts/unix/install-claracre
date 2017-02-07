#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 1.13.17


if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME environmental variable is not defined. Exiting..."
    exit
fi

command_exists () {
    type "$1" &> /dev/null ;
}

OS="`uname`"
case $OS in
  'Linux')

  if ! command_exists wget ; then
  echo "Can not run wget. Exiting..."
  exit
  fi

    wget https://userweb.jlab.org/~gurjyan/clara-cre/clara-cre.tar.gz
    tar xvzf clara-cre.tar.gz
      MACHINE_TYPE=`uname -m`
      if [ ${MACHINE_TYPE} == 'x86_64' ]; then
    wget https://userweb.jlab.org/~gurjyan/clara-cre/linux-64.tar.gz
  else
     wget https://userweb.jlab.org/~gurjyan/clara-cre/linux-i586.tar.gz
  fi
    ;;
#  'WindowsNT')
#    OS='Windows'
#    ;;
  'Darwin')

    if ! command_exists curl ; then
    echo "Can not run curl. Exiting..."
    exit
    fi

 curl "https://userweb.jlab.org/~gurjyan/clara-cre/clara-cre.tar.gz" -o clara-cre.tar.gz

 curl "https://userweb.jlab.org/~gurjyan/clara-cre/macosx-64.tar.gz" -o macosx-64.tar.gz
    ;;
  *) ;;
esac

    tar xvzf clara-cre.tar.gz
    rm -f clara-cre.tar.gz
    cd clara-cre
    mkdir jre
    cd jre
    mv ../../*.tar.* .

echo "Installing jre ..."
tar xvzf *.tar.*

cd ../..
echo `pwd`

rm -rf $CLARA_HOME
mv clara-cre $CLARA_HOME
rm -rf clara-cre

echo done
