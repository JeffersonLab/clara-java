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
./gradle deploy
)

(
echo "Downloading and building clara-java package ..."
git clone --depth 1 https://github.com/JeffersonLab/clara-java.git
cd clara-java || exit
./gradlew install && ./gradlew deploy
)

echo "Done!"
