#!/bin/bash

function do_help()
{
    echo -e "Usage:
       install-script.sh -d <directory of UX Home> -u <the user to startup UX>

       For example, UX is installed in /opt/uxdb11.5 and should be startup with user uxdb
       The option -d is mandatory
       Please run the script as:

       ./install-script.sh -d /opt/uxdb11.5 -u uxdb\n" 
}


UXdir=""

while getopts "d:h" arg 
do
    case $arg in
    d)
        UXdir=$OPTARG
        echo $UXdir
        ;;
    h)
        do_help
        ;;
    ?)
        echo "unkonw argument"
        exit 1
        ;;
    esac
done

if [ "$UXdir" = "" ]
then
    do_help
    exit 1
fi

if [ "$(id -u)" != "0" ]
then
    echo "Error: please use root user to run this cmd"
    exit 1
fi

cp ./repmgr $UXdir/bin
cp ./repmgrd $UXdir/bin
cp ./repmgr.so $UXdir/lib
chmod 755 $UXdir/bin/repmgr
chmod 755 $UXdir/bin/repmgrd
chmod 755 $UXdir/lib/repmgr.so
chmod 644 $UXdir/conf/ux_repmgr.conf
cp ./repmgr*.sql $UXdir/share/extension/
cp ./repmgr.control $UXdir/share/extension/
chmod 644 $UXdir/share/extension/repmgr.control
chmod 644 $UXdir/share/extension/repmgr*.sql
mkdir $UXdir/conf
touch $UXdir/conf/ux_repmgr.conf

