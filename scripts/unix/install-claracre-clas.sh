#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 1.13.17


if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME environmental variable is not defined. Exiting..."
    exit
fi

rm -rf $CLARA_HOME

PLUGIN=coatjava-4a.2.2

    case "$1" in
      -u | --update)
      if ! [ -z ${2+x} ]; then PLUGIN=$2; fi
      echo $PLUGIN
	  ;;
    esac

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
    wget http://clasweb.jlab.org/clas12offline/distribution/coatjava/"$PLUGIN".tar.gz


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
 curl "http://clasweb.jlab.org/clas12offline/distribution/coatjava/"$PLUGIN".tar.gz" -o "$PLUGIN".tar.gz

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
    mv "$PLUGIN".tar.gz ../../.



echo "Installing jre ..."
tar xvzf *.tar.*
rm -f *.tar.*

cd ../..

mv clara-cre $CLARA_HOME

tar xvzf "$PLUGIN".tar.gz


cd coatjava

cp -r etc $CLARA_HOME/plugins/clas12/.
cp -r bin $CLARA_HOME/plugins/clas12/.
cp -r lib/packages $CLARA_HOME/plugins/clas12/lib/
cp -r lib/utils $CLARA_HOME/plugins/clas12/lib/
cp  lib/clas/* $CLARA_HOME/plugins/clas12/lib/clas/.
cp  lib/services/* $CLARA_HOME/plugins/clas12/lib/services/.

rm -f $CLARA_HOME/plugins/clas12/lib/services/.*.jar
rm -f $CLARA_HOME/plugins/clas12/lib/clas/.*.jar

rm -f $CLARA_HOME/plugins/clas12/lib/clas/commons-exec*.jar
rm -f $CLARA_HOME/plugins/clas12/lib/clas/jsap*.jar
rm -f $CLARA_HOME/plugins/clas12/lib/clas/json*.jar
rm -f $CLARA_HOME/plugins/clas12/lib/clas/snakeyaml*.jar


rm -f $CLARA_HOME/plugins/clas12/bin/clara-rec
rm -f $CLARA_HOME/plugins/clas12/README
cp $CLARA_HOME/plugins/clas12/etc/services/reconstruction.yaml $CLARA_HOME/plugins/clas12/config/services.yaml
rm -rf $CLARA_HOME/plugins/clas12/etc/services

cd ..
rm -rf coatjava
rm "$PLUGIN".tar.gz

chmod a+x $CLARA_HOME/bin/*
chmod a+x $CLARA_HOME/bin/etc/*


echo done
