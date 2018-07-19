#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 1.13.17

is_local="false"

if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME environmental variable is not defined. Exiting..."
    exit
fi

rm -rf "$CLARA_HOME"

PLUGIN=5a.2.0

case "$1" in
    -v | --version)
        if ! [ -z "${2+x}" ]; then PLUGIN=$2; fi
        echo "$PLUGIN"
        ;;
    -l | --local)
        if ! [ -z "${2+x}" ]; then PLUGIN=$2; is_local="true"; fi
        echo "$PLUGIN"
        ;;
esac

command_exists () {
    type "$1" &> /dev/null ;
}

OS=$(uname)
case $OS in
    'Linux')

        if ! command_exists wget ; then
            echo "Can not run wget. Exiting..."
            exit
        fi

        wget https://userweb.jlab.org/~gurjyan/clara-cre/clara-cre.tar.gz

        if [ "$is_local" == "false" ]; then
            wget https://clasweb.jlab.org/clas12offline/distribution/coatjava/coatjava-$PLUGIN.tar.gz
            wget https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-1.0.tar.gz
        else
            cp $PLUGIN .
        fi

        MACHINE_TYPE=$(uname -m)
        if [ "$MACHINE_TYPE" == "x86_64" ]; then
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

       if [ "$is_local" == "false" ]; then
            curl "https://clasweb.jlab.org/clas12offline/distribution/coatjava/coatjava-$PLUGIN.tar.gz" -o coatjava-$PLUGIN.tar.gz
            curl "https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-1.0.tar.gz" -o grapes-1.0.tar.gz
       else
            cp $PLUGIN .
       fi

        curl "https://userweb.jlab.org/~gurjyan/clara-cre/macosx-64.tar.gz" -o macosx-64.tar.gz
        ;;

    *) ;;
esac

tar xvzf clara-cre.tar.gz
rm -f clara-cre.tar.gz

(
mkdir clara-cre/jre
cd clara-cre/jre || exit

mv ../../*.tar.* .
mv coatjava-$PLUGIN.tar.gz ../../.

echo "Installing jre ..."
tar xvzf ./*.tar.*
rm -f ./*.tar.*
)

mv clara-cre "$CLARA_HOME"

tar xvzf coatjava-$PLUGIN.tar.gz

(
cd coatjava || exit
cp -r etc "$CLARA_HOME"/plugins/clas12/.
cp -r bin "$CLARA_HOME"/plugins/clas12/.
cp lib/clas/* "$CLARA_HOME"/plugins/clas12/lib/clas/.
cp lib/services/* "$CLARA_HOME"/plugins/clas12/lib/services/.
)

tar xvzf grapes-1.0.tar.gz
mv grapes-1.0 "$CLARA_HOME"/plugins/grapes

cp "$CLARA_HOME"/plugins/grapes/bin/clara-grapes "$CLARA_HOME"/bin/.


rm -f "$CLARA_HOME"/plugins/clas12/bin/clara-rec
rm -f "$CLARA_HOME"/plugins/clas12/README
cp "$CLARA_HOME"/plugins/clas12/etc/services/reconstruction.yaml "$CLARA_HOME"/plugins/clas12/config/services.yaml
rm -rf "$CLARA_HOME"/plugins/clas12/etc/services

rm -rf coatjava
rm coatjava-$PLUGIN.tar.gz

chmod a+x "$CLARA_HOME"/bin/*

echo "Done!"
