#!/bin/bash

################################################################################
#
# file name              : release_repmgr.sh
# purpose                : automatically compile and do the release stuffs
#
################################################################################

# If we reference any variable not initialized, or there is any error occurred
# then we can exit immediately.
[ ! -z "$UXDB_INSTALL" ] && { echo "uxdb install path already set to $UXDB_INSTALL" ; } ||\
    { export UXDB_INSTALL=/home/uxdb/uxdbinstall; echo "set uxdb install path: $UXDB_INSTALL";}
set -eu

WORK_HOME=$(pwd)
PROJ_HOME=$WORK_HOME/..
UXDB_HOME=$UXDB_INSTALL/dbsql
REPMGR_HOME=$UXDB_INSTALL/dbsql/share/extension
#number of multithreads
MULTITHREADS="-j $(nproc)"
################################################################################
#
# functions
#
################################################################################
function show_usage()
{
	echo "Usage:"
	echo "release_repmgr.sh --version=x.x.x.x, version number to be released"
	echo "                  --help, this information"
}

################################################################################
#
# parse arguments
#
################################################################################
ARGUMENTS=$(getopt -o hv: --long version:,debug,help -n "release_repmgr.sh" -- "$@")
if [ $? != 0 ]; then
    echo "Argument parse error, Terminating..."
    exit 1
fi

# save normalized command arguments to positional arguments ($1, $2, ...)
eval set -- "${ARGUMENTS}"

#
# default values
#
HELP="no"

# --version, release version number[5.0]
VERSION="5.0"

while true
do
    case "$1" in
	-h|--help)
		HELP="yes"

		shift
		break
		;;

	-v|--version)
		VERSION=$2
		shift 2
		;;
	--)
		shift
		break
		;;
	*)
		echo "Internal error!"
		exit 1
		;;
	esac
done

if [ "$HELP" == "yes" ] ; then
	show_usage
	exit 0
fi

################################################################################
#
# configure & build & install repmgr
#
################################################################################
cd ${PROJ_HOME}
autoreconf -fiv
UX_CONFIG=${UXDB_INSTALL}/dbsql/bin/ux_config ./configure
make $MULTITHREADS UXFS=no
make $MULTITHREADS install
cp repmgr.conf.sample ${UXDB_INSTALL}/dbsql/share/


################################################################################
#
# pack a tar ball
#
################################################################################
cd ${WORK_HOME}
sed -i "/VERSION=/c\VERSION=$VERSION" install.sh
echo "Notice: current repmgr release version ${VERSION}"

mkdir -p repmgr-$VERSION/lib
mkdir -p repmgr-$VERSION/extension
mkdir -p repmgr-$VERSION/bin
cp -rp ${UXDB_HOME}/lib/repmgr.so ./repmgr-$VERSION/lib
cp -rp ${REPMGR_HOME}/repmgr* ./repmgr-$VERSION/extension
cp -rp ${UXDB_HOME}/bin/repmgr ${UXDB_HOME}/bin/repmgrd ./repmgr-$VERSION/bin
cp -rp ${UXDB_HOME}/bin/repmgr.conf ${UXDB_HOME}/bin/repmgr_service ${UXDB_HOME}/bin/uxdb_service ./repmgr-$VERSION/bin
tar -zcvf repmgr-$VERSION.tar repmgr-$VERSION 


################################################################################
#
# do the release stuffs
#
################################################################################
# CENTOS=`cat /etc/redhat-release | awk -F 'release ' '{ print $2 }' | awk -F ' ' '{ printf $1}'`
# MAJOR=`expr $CENTOS : '\([0-9]\)'`

release()
{
	cp -rp repmgr-$VERSION.tar 		repmgr-linux-v${VERSION}/
	cp -rp install.sh				repmgr-linux-v${VERSION}/
	cp -rp uninstall.sh 			repmgr-linux-v${VERSION}/
	cp -rp platform					repmgr-linux-v${VERSION}/
	tar -cjvf repmgr-linux-v${VERSION}.tar.bz2 repmgr-linux-v${VERSION}/
}

mkdir -p repmgr-linux-v${VERSION}
echo "mkdir repmgr-linux-v${VERSION}"
release

rm -rf repmgr-$VERSION
rm -rf repmgr-linux-v${VERSION}

