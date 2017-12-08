#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 1.13.17

if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME environmental variable is not defined. Exiting..."
    exit
fi

rm -rf "$CLARA_HOME"

mkdir "$CLARA_HOME"
mkdir "$CLARA_HOME"/plugins
mkdir "$CLARA_HOME"/plugins/clas12
mkdir "$CLARA_HOME"/plugins/clas12/config

PLUGIN=4a.8.4

while :
do
    case "$1" in
        -v | --version)
            if ! [ -z "${2+x}" ]; then PLUGIN=$2; fi
            echo "$PLUGIN"
            break
            ;;

        *)  # No more options
            break
            ;;
    esac
done

command_exists () {
    type "$1" &> /dev/null ;
}

if ! command_exists git ; then
    echo "Can not run git. Exiting..."
    exit
fi

rm -rf clara-dk

echo "Creating clara working directory ..."
mkdir clara-dk
cd clara-dk || exit

(
echo "Downloading and building xMsg package ..."
git clone --depth 1 https://github.com/JeffersonLab/xmsg-java.git
cd xmsg-java || exit
./gradlew install
)

(
echo "Downloading and building jinflux package ..."
git clone --depth 1 https://github.com/JeffersonLab/JinFlux.git
cd JinFlux || exit
gradle install
gradle deploy
)

(
echo "Downloading and building clara-java package ..."
git clone --depth 1 https://github.com/JeffersonLab/clara-java.git
cd clara-java || exit
./gradlew install
./gradlew deploy
)

echo "Downloading coat-java and jre packages ..."
mkdir "$CLARA_HOME"/jre

OS=$(uname)
case $OS in
    'Linux')
         wget http://clasweb.jlab.org/clas12offline/distribution/coatjava/coatjava-$PLUGIN.tar.gz
        MACHINE_TYPE=$(uname -m)
        if [ "$MACHINE_TYPE" == "x86_64" ]; then
            wget https://userweb.jlab.org/~gurjyan/clara-cre/linux-64.tar.gz
            mv linux-64.tar.gz "$CLARA_HOME"/jre
        else
            wget https://userweb.jlab.org/~gurjyan/clara-cre/linux-i586.tar.gz
            mv linux-i586.tar.gz "$CLARA_HOME"/jre
        fi
        ;;

    #  'WindowsNT')
        #    OS='Windows'
        #    ;;

    'Darwin')
        curl "http://clasweb.jlab.org/clas12offline/distribution/coatjava/coatjava-$PLUGIN.tar.gz" -o coatjava-$PLUGIN.tar.gz

        curl "https://userweb.jlab.org/~gurjyan/clara-cre/macosx-64.tar.gz" -o macosx-64.tar.gz
        mv macosx-64.tar.gz "$CLARA_HOME"/jre
        ;;

    *) ;;
esac

tar xvzf coatjava-$PLUGIN.tar.gz

(
cd coatjava || exit
cp -r etc "$CLARA_HOME"/plugins/clas12/.
cp -r bin "$CLARA_HOME"/plugins/clas12/.
cp -r scripts "$CLARA_HOME"/plugins/clas12/.
cp -r lib "$CLARA_HOME"/plugins/clas12/.
rm -f "$CLARA_HOME"/plugins/clas12/bin/clara-rec
rm -f "$CLARA_HOME"/plugins/clas12/README
)

echo "Downloading and building clasrec-io package ..."
git clone --depth 1 https://github.com/JeffersonLab/clasrec-io.git
(
cd clasrec-io || exit
./gradlew deploy
)

echo "Downloading and building clasrec-orchestrators package ..."
git clone --depth 1 https://github.com/JeffersonLab/clasrec-orchestrators.git
(
cd clasrec-orchestrators || exit
./gradlew deploy
)

rm -rf "$CLARA_HOME"/plugins/clas12/etc/services

mkdir "$CLARA_HOME"/data
mkdir "$CLARA_HOME"/data/input
mkdir "$CLARA_HOME"/data/output

cp /group/da/vhg/data/* "$CLARA_HOME"/data/input/

echo "Installing jre ..."
cd "$CLARA_HOME"/jre || exit
tar xvzf ./*.tar.*
rm -f ./*.tar.*

hash -r

echo "Done!"
