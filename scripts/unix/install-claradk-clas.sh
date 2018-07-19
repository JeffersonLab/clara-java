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
mkdir "$CLARA_HOME"/plugins/grapes
mkdir "$CLARA_HOME"/plugins/clas12/config
mkdir "$CLARA_HOME"/data
mkdir "$CLARA_HOME"/data/input
mkdir "$CLARA_HOME"/data/output

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

echo "Downloading and building clasrec-io package ..."
git clone --depth 1 https://github.com/JeffersonLab/clasrec-io.git
(
cd clasrec-io || exit
./gradlew deploy
)

echo "Done!"
