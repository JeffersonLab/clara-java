#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 1.13.19

is_local="false"

case "$1" in
    -h | --help)
        echo "usage: install-claracre-clas.sh [ OPTION ]... [ Value ]..."
        echo ""
        echo " -h, --help         print this help."
        echo " -f, --framework    Clara framework version (default = 4.4)."
        echo " -v, --version      Clas12 plugin version   (default = 6b.1.1)."
        echo " -g, --grapes       Grapes plugin version   (default = 2.1)."
        exit 1
        ;;
esac

if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME environmental variable is not defined. Exiting..."
    exit 0
fi

if  [ -d "${CLARA_HOME}" ]; then
  echo "The old installation at $CLARA_HOME it will be deleted."
  read -n 1 -p "Do you want to continue? Y/N `echo $'\n> '`" uinput
   if [ "$uinput" != "Y" ]; then
     exit 0
   else
# check to see if CLARA_HOME points to a valid clara distribution dir
       if [ -d "${CLARA_HOME}/plugins" ]; then
         rm -rf "$CLARA_HOME"
       else
         echo "CLARA_HOME is not a valid Clara distribution. Exiting..."
         exit 0
       fi
   fi
fi

PLUGIN=6b.2.0
GRAPES=2.1
FV=4.3.11

case "$1" in
    -f | --framework)
        if ! [ -z "${2+x}" ]; then FV=$2; fi
        echo "CLARA version = $FV"
        ;;
    -v | --version)
        if ! [ -z "${2+x}" ]; then PLUGIN=$2; fi
        echo "CLAS12 plugin version = $PLUGIN"
        ;;
    -g | --grapes)
        if ! [ -z "${2+x}" ]; then GRAPES=$2; fi
        echo "Grapes plugin version = $GRAPES"
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
    -g | --grapes)
        if ! [ -z "${4+x}" ]; then GRAPES=$4; fi
        echo "Grapes plugin version = $GRAPES"
        ;;
    -l | --local)
        if ! [ -z "${4+x}" ]; then PLUGIN=$4; is_local="true"; fi
        echo "CLAS12 plugin = $PLUGIN"
        ;;
esac
case "$5" in
    -f | --framework)
        if ! [ -z "${6+x}" ]; then FV=$6; fi
        echo "CLARA version = $FV"
        ;;
    -v | --version)
        if ! [ -z "${6+x}" ]; then PLUGIN=$6; fi
        echo "CLAS12 plugin version = $PLUGIN"
        ;;
    -g | --grapes)
        if ! [ -z "${6+x}" ]; then GRAPES=$6; fi
        echo "Grapes plugin version = $GRAPES"
        ;;
    -l | --local)
        if ! [ -z "${6+x}" ]; then PLUGIN=$6; is_local="true"; fi
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
            echo "getting grapes-$GRAPES"
            wget https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-$GRAPES.tar.gz
        else
            echo "getting grapes-$GRAPES"
            wget https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-$GRAPES.tar.gz
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
            echo "getting grapes-$GRAPES"
            curl "https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-$GRAPES.tar.gz" -o grapes-$GRAPES.tar.gz
       else
            echo "getting grapes-$GRAPES"
            curl "https://clasweb.jlab.org/clas12offline/distribution/grapes/grapes-$GRAPES.tar.gz" -o grapes-$GRAPES.tar.gz
            cp $PLUGIN .
       fi

        curl "https://userweb.jlab.org/~gurjyan/clara-cre/macosx-64.tar.gz" -o macosx-64.tar.gz
        ;;

    *) ;;
esac

tar xvzf clara-cre-$FV.tar.gz
rm -f clara-cre-$FV.tar.gz

mkdir clara-cre/jre
(
cd clara-cre/jre || exit
echo "Installing jre ..."
case $OS in
    'Linux')
    if [ "$MACHINE_TYPE" == "x86_64" ]; then
         mv ../../linux-64.tar.gz .
         tar xvzf ./linux-64.tar.gz
         rm linux-64.tar.gz
    else
        mv ../../linux-i586.tar.gz .
        tar xvzf ./linux-i586.tar.gz
        rm linux-i586.tar.gz
    fi
    ;;

    'Darwin')
    mv ../../macosx-64.tar.gz .
    tar xvzf ./macosx-64.tar.gz
    rm macosx-64.tar.gz
    ;;
    *) ;;
esac
)

mv clara-cre "$CLARA_HOME"

echo "Installing coatjava ..."
tar xvzf coatjava-$PLUGIN.tar.gz
(
cd coatjava || exit
cp -r etc "$CLARA_HOME"/plugins/clas12/.
cp -r bin "$CLARA_HOME"/plugins/clas12/.
cp -r lib/utils "$CLARA_HOME"/plugins/clas12/lib/.
cp lib/clas/* "$CLARA_HOME"/plugins/clas12/lib/clas/.
cp lib/services/* "$CLARA_HOME"/plugins/clas12/lib/services/.
)
rm -rf coatjava
rm coatjava-$PLUGIN.tar.gz

echo "Installing grapes ..."
tar xvzf grapes-$GRAPES.tar.gz
mv grapes-$GRAPES "$CLARA_HOME"/plugins/grapes
cp "$CLARA_HOME"/plugins/grapes/bin/clara-grapes "$CLARA_HOME"/bin/.
rm -f "$CLARA_HOME"/plugins/clas12/bin/clara-rec
rm -f "$CLARA_HOME"/plugins/clas12/README
cp "$CLARA_HOME"/plugins/clas12/etc/services/*.yaml "$CLARA_HOME"/plugins/clas12/config/.
mv "$CLARA_HOME"/plugins/clas12/config/reconstruction.yaml "$CLARA_HOME"/plugins/clas12/config/services.yaml
rm -rf "$CLARA_HOME"/plugins/clas12/etc/services
rm grapes-$GRAPES.tar.gz

chmod a+x "$CLARA_HOME"/bin/*

echo "Clara Framework  :    clara-cre-$FV" > "$CLARA_HOME"/.version
echo "CLAS12 plugin    :    coatjava-$PLUGIN" >> "$CLARA_HOME"/.version
echo "Grapes plugin    :    grapes-$GRAPES" >> "$CLARA_HOME"/.version
echo "Done!"
