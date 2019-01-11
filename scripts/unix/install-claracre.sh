#!/usr/bin/env bash
# author Vardan Gyurjyan
# date 1.10.19

is_local="false"

if ! [ -n "$CLARA_HOME" ]; then
    echo "CLARA_HOME environmental variable is not defined. Exiting..."
    exit
fi

echo "If you have an old installation at $CLARA_HOME it will be deleted."
read -n 1 -p "Do you want to continue? Y/N `echo $'\n> '`" uinput
if [ "$uinput" != "Y" ]; then exit 0
fi

rm -rf "$CLARA_HOME"

FV=4.3.5

case "$1" in
    -f | --framework)
        if ! [ -z "${2+x}" ]; then FV=$2; fi
        echo "CLARA version = $FV"
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
echo "Installing jre ..."
tar xvzf ./*.tar.*
rm -f ./*.tar.*
)

mv clara-cre "$CLARA_HOME"

chmod a+x "$CLARA_HOME"/bin/*

rm -rf $CLARA_HOME/plugins/clas12

echo "Distribution  :    clara-cre-$FV" > "$CLARA_HOME"/.version
echo "Done!"
