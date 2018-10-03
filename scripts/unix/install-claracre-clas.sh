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
FV=4.3.3

case "$1" in
    -f | --framework)
        if ! [ -z "${2+x}" ]; then FV=$2; fi
        echo "CLARA version = $FV"
        ;;
    -v | --version)
        if ! [ -z "${2+x}" ]; then PLUGIN=$2; fi
        echo "CLAS12 plugin version = $PLUGIN"
        ;;
    -l | --local)
        if ! [ -z "${2+x}" ]; then PLUGIN=$2; is_local="true"; fi
        echo "CLAS12 plugin = $PLUGIN"
        ;;
esac
case "$3" in
    -f | --framework)
        if ! [ -z "${4+x}" ]; then FV=$4; fi
        echo "CLARA version = $FV"
        ;;
    -v | --version)
        if ! [ -z "${4+x}" ]; then PLUGIN=$4; fi
        echo "CLAS12 plugin version = $PLUGIN"
        ;;
    -l | --local)
        if ! [ -z "${4+x}" ]; then PLUGIN=$4; is_local="true"; fi
        echo "CLAS12 plugin = $PLUGIN"
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

        wget https://userweb.jlab.org/~gurjyan/clara-cre/clara-cre-$FV.tar.gz

        if [ "$is_local" == "false" ]; then
            echo "getting coatjava-$PLUGIN"
            wget https://clasweb.jlab.org/clas12offline/distribution/coatjava/coatjava-$PLUGIN.tar.gz
            echo "getting grapes-1.0"
            wget https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-1.0.tar.gz
        else
            echo "getting grapes-1.0"
            wget https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-1.0.tar.gz
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

        curl "https://userweb.jlab.org/~gurjyan/clara-cre/clara-cre-$FV.tar.gz" -o clara-cre-$FV.tar.gz

       if [ "$is_local" == "false" ]; then
            echo "getting coatjava-$PLUGIN"
            curl "https://clasweb.jlab.org/clas12offline/distribution/coatjava/coatjava-$PLUGIN.tar.gz" -o coatjava-$PLUGIN.tar.gz
            echo "getting grapes-1.0"
            curl "https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-1.0.tar.gz" -o grapes-1.0.tar.gz
       else
            echo "getting grapes-1.0"
            curl "https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-1.0.tar.gz" -o grapes-1.0.tar.gz
            cp $PLUGIN .
       fi

        curl "https://userweb.jlab.org/~gurjyan/clara-cre/macosx-64.tar.gz" -o macosx-64.tar.gz
        ;;

    *) ;;
esac

tar xvzf clara-cre-$FV.tar.gz
rm -f clara-cre-$FV.tar.gz

(
mkdir clara-cre/jre
cd clara-cre/jre || exit

mv ../../*.tar.* .
mv coatjava-$PLUGIN.tar.gz ../../.
mv grapes-1.0.tar.gz ../../.
echo "Installing jre ..."
tar xvzf ./*.tar.*
rm -f ./*.tar.*
)

mv clara-cre "$CLARA_HOME"

echo "Installing coatjava ..."
tar xvzf coatjava-$PLUGIN.tar.gz

(
cd coatjava || exit
cp -r etc "$CLARA_HOME"/plugins/clas12/.
cp -r bin "$CLARA_HOME"/plugins/clas12/.
cp lib/clas/* "$CLARA_HOME"/plugins/clas12/lib/clas/.
cp lib/services/* "$CLARA_HOME"/plugins/clas12/lib/services/.
)

echo "Installing grapes ..."
tar xvzf grapes-1.0.tar.gz
mv grapes-1.0 "$CLARA_HOME"/plugins/grapes

cp "$CLARA_HOME"/plugins/grapes/bin/clara-grapes "$CLARA_HOME"/bin/.


rm -f "$CLARA_HOME"/plugins/clas12/bin/clara-rec
rm -f "$CLARA_HOME"/plugins/clas12/README
cp "$CLARA_HOME"/plugins/clas12/etc/services/*.yaml "$CLARA_HOME"/plugins/clas12/config/.
mv "$CLARA_HOME"/plugins/clas12/config/reconstruction.yaml "$CLARA_HOME"/plugins/clas12/config/services.yaml
rm -rf "$CLARA_HOME"/plugins/clas12/etc/services

rm -rf coatjava
rm coatjava-$PLUGIN.tar.gz
rm grapes-1.0.tar.gz

chmod a+x "$CLARA_HOME"/bin/*

echo "Distribution  :    clara-cre-$FV" > "$CLARA_HOME"/.version
echo "CLAS12 plugin :    coatjava-$PLUGIN" >> "$CLARA_HOME"/.version
echo "Done!"
